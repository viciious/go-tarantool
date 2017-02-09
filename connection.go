package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phonkee/godsn"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	DefaultSpace   string
	User           string
	Password       string
}

type Greeting struct {
	Version []byte
	Auth    []byte
}

type Connection struct {
	dsn       *godsn.DSN
	requestID uint32
	requests  *requestMap
	writeChan chan []byte // packed messages with header
	closeOnce sync.Once
	exit      chan bool
	closed    chan bool
	tcpConn   net.Conn
	// options
	queryTimeout time.Duration
	Greeting     *Greeting
	packData     *packData
}

func Connect(dsnString string, options *Options) (conn *Connection, err error) {
	var dsn *godsn.DSN

	defer func() { // close opened connection if error
		if err != nil && conn != nil {
			if conn.tcpConn != nil {
				conn.tcpConn.Close()
			}
			conn = nil
		}
	}()

	// remove schema, if present
	if strings.HasPrefix(dsnString, "tcp://") {
		dsn, err = godsn.Parse(strings.Split(dsnString, "tcp:")[1])
	} else if strings.HasPrefix(dsnString, "unix://") {
		dsn, err = godsn.Parse(strings.Split(dsnString, "unix:")[1])
	} else {
		dsn, err = godsn.Parse("//" + dsnString)
	}

	if err != nil {
		return nil, err
	}

	conn = &Connection{
		dsn:       dsn,
		requests:  newRequestMap(),
		writeChan: make(chan []byte, 256),
		exit:      make(chan bool),
		closed:    make(chan bool),
	}

	if options == nil {
		options = &Options{}
	}

	opts := *options // copy to new object

	if opts.ConnectTimeout.Nanoseconds() == 0 {
		opts.ConnectTimeout = time.Duration(time.Second)
	}

	if opts.QueryTimeout.Nanoseconds() == 0 {
		opts.QueryTimeout = time.Duration(time.Second)
	}

	if options.User == "" {
		user := dsn.User()
		if user != nil {
			username := user.Username()
			pass, _ := user.Password()
			options.User = username
			options.Password = pass
		}
	}

	remoteAddr := dsn.Host()
	path := dsn.Path()

	if opts.DefaultSpace == "" {
		if len(path) > 0 {
			splitPath := strings.Split(path, "/")
			if len(splitPath) > 1 {
				if splitPath[1] == "" {
					return nil, fmt.Errorf("Wrong space: %s", splitPath[1])
				}
				opts.DefaultSpace = splitPath[1]
			}
		}
	}

	d, err := newPackData(opts.DefaultSpace)
	if err != nil {
		return nil, err
	}
	conn.packData = d

	conn.queryTimeout = opts.QueryTimeout

	connectDeadline := time.Now().Add(opts.ConnectTimeout)

	conn.tcpConn, err = net.DialTimeout("tcp", remoteAddr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	greeting := make([]byte, 128)

	conn.tcpConn.SetDeadline(connectDeadline)

	_, err = io.ReadFull(conn.tcpConn, greeting)
	if err != nil {
		return
	}

	read := func(r io.Reader) (*Packet, error) {
		body, rerr := readMessage(r)
		if rerr != nil {
			return nil, rerr
		}

		packet, rerr := decodePacket(bytes.NewBuffer(body))
		if rerr != nil {
			return nil, rerr
		}

		return packet, nil
	}

	conn.Greeting = &Greeting{
		Version: greeting[:64],
		Auth:    greeting[64:108],
	}

	if options.User != "" {
		var authCode byte
		var authRaw []byte
		var authResponse *Packet

		requestID := conn.nextID()

		authCode, authRaw, err = (&Auth{
			User:         options.User,
			Password:     options.Password,
			GreetingAuth: conn.Greeting.Auth,
		}).Pack(conn.packData)
		if err != nil {
			return
		}

		_, err = conn.tcpConn.Write(packIproto(authCode, requestID, authRaw))
		if err != nil {
			return
		}

		authResponse, err = read(conn.tcpConn)
		if err != nil {
			return
		}

		if authResponse.requestID != requestID {
			err = errors.New("Bad auth responseID")
			return
		}

		if authResponse.result != nil && authResponse.result.Error != nil {
			err = authResponse.result.Error
			return
		}
	}

	// select space and index schema
	request := func(req Query) (*Result, error) {
		var err error
		requestID := conn.nextID()
		packedCode, packedReq, err := (req).Pack(conn.packData)
		if err != nil {
			return nil, err
		}

		_, err = conn.tcpConn.Write(packIproto(packedCode, requestID, packedReq))
		if err != nil {
			return nil, err
		}

		response, err := read(conn.tcpConn)
		if err != nil {
			return nil, err
		}

		if response.requestID != requestID {
			return nil, errors.New("Bad response requestID")
		}

		if response.result == nil {
			return nil, errors.New("Nil response result")
		}

		if response.result.Error != nil {
			return nil, response.result.Error
		}

		return response.result, nil
	}

	res, err := request(&Select{
		Space:    ViewSpace,
		Key:      0,
		Iterator: IterAll,
	})
	if err != nil {
		return
	}

	for _, space := range res.Data {
		conn.packData.spaceMap[space[2].(string)] = space[0].(uint64)
	}

	var defSpaceBuf bytes.Buffer
	defSpaceEnc := msgpack.NewEncoder(&defSpaceBuf)
	conn.packData.encodeSpace(opts.DefaultSpace, defSpaceEnc)
	conn.packData.packedDefaultSpace = defSpaceBuf.Bytes()

	res, err = request(&Select{
		Space:    ViewIndex,
		Key:      0,
		Iterator: IterAll,
	})
	if err != nil {
		return
	}

	for _, index := range res.Data {
		spaceID := index[0].(uint64)
		indexSpaceMap, exists := conn.packData.indexMap[spaceID]
		if !exists {
			indexSpaceMap = make(map[string]uint64)
			conn.packData.indexMap[spaceID] = indexSpaceMap
		}
		indexSpaceMap[index[2].(string)] = index[1].(uint64)
	}

	// remove deadline
	conn.tcpConn.SetDeadline(time.Time{})

	go conn.worker(conn.tcpConn)

	return
}

func (conn *Connection) nextID() uint32 {
	return atomic.AddUint32(&conn.requestID, 1)
}

func (conn *Connection) stop() {
	conn.closeOnce.Do(func() {
		// debug.PrintStack()
		close(conn.exit)
		conn.tcpConn.Close()
	})
}

func (conn *Connection) Close() {
	conn.stop()
	<-conn.closed
}

func (conn *Connection) IsClosed() bool {
	select {
	case <-conn.exit:
		return true
	default:
		return false
	}
}

func (conn *Connection) worker(tcpConn net.Conn) {

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		writer(tcpConn, conn.writeChan, conn.exit)
		conn.stop()
		wg.Done()
	}()

	go func() {
		conn.reader(tcpConn)
		conn.stop()
		wg.Done()
	}()

	wg.Wait()

	// send error reply to all pending requests
	conn.requests.CleanUp(func(req *request) {
		req.replyChan <- &Result{
			Error: ConnectionClosedError(conn),
		}
		close(req.replyChan)
	})

	close(conn.closed)
}

func writer(tcpConn net.Conn, writeChan chan []byte, stopChan chan bool) {
	var err error
	var n int

	w := bufio.NewWriter(tcpConn)

WRITER_LOOP:
	for {
		select {
		case messageBody, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			n, err = w.Write(messageBody)
			if err != nil || n != len(messageBody) {
				break WRITER_LOOP
			}
		case <-stopChan:
			break WRITER_LOOP
		default:
			if err = w.Flush(); err != nil {
				break WRITER_LOOP
			}

			// same without flush
			select {
			case messageBody, ok := <-writeChan:
				if !ok {
					break WRITER_LOOP
				}
				n, err = w.Write(messageBody)
				if err != nil || n != len(messageBody) {
					break WRITER_LOOP
				}
			case <-stopChan:
				break WRITER_LOOP
			}

		}
	}
	if err != nil {
		// @TODO
		// pp.Println(err)
	}
}

func (conn *Connection) reader(tcpConn net.Conn) {
	var packet *Packet
	var err error
	var body []byte
	var req *request

	r := bufio.NewReaderSize(tcpConn, 128*1024)

READER_LOOP:
	for {
		// read raw bytes
		body, err = readMessage(r)
		if err != nil {
			break READER_LOOP
		}

		packet = &Packet{
			buf: bytes.NewBuffer(body),
		}

		// decode packet header for requestID
		err = packet.decodeHeader(packet.buf)
		if err != nil {
			break READER_LOOP
		}

		req = conn.requests.Pop(packet.requestID)
		if req != nil {
			res := &Result{}
			// finish decode message body
			err = packet.decodeBody(packet.buf)
			if err != nil {
				res.Error = err
			} else if packet.result != nil {
				res = packet.result
			}

			req.replyChan <- res
			close(req.replyChan)
		}
	}
}
