package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
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
	writeChan chan *packedPacket // packed messages with header
	closeOnce sync.Once
	exit      chan bool
	closed    chan bool
	tcpConn   net.Conn
	// options
	queryTimeout   time.Duration
	Greeting       *Greeting
	packData       *packData
	remoteAddr     string
	firstError     error
	firstErrorLock *sync.Mutex
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
		dsn:            dsn,
		requests:       newRequestMap(),
		writeChan:      make(chan *packedPacket, 256),
		exit:           make(chan bool),
		closed:         make(chan bool),
		firstErrorLock: &sync.Mutex{},
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

	conn.remoteAddr = remoteAddr
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
		pp, rerr := readPacked(r)
		if rerr != nil {
			return nil, rerr
		}
		defer pp.Release()

		packet, rerr := decodePacket(pp)
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
		var authResponse *Packet

		requestID := conn.nextID()

		pp := packIproto(0, requestID)
		defer pp.Release()

		pp.code, err = (&Auth{
			User:         options.User,
			Password:     options.Password,
			GreetingAuth: conn.Greeting.Auth,
		}).Pack(conn.packData, pp.poolBuffer.buffer)
		if err != nil {
			return
		}

		_, err = pp.WriteTo(conn.tcpConn)
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

		pp := packIproto(0, requestID)
		defer pp.Release()

		pp.code, err = (req).Pack(conn.packData, pp.poolBuffer.buffer)
		if err != nil {
			return nil, err
		}

		_, err = pp.WriteTo(conn.tcpConn)
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
		indexID := index[1].(uint64)
		indexName := index[2].(string)
		indexAttr := index[4].(map[interface{}]interface{}) // e.g: {"unique": true}
		indexFields := index[5].([]interface{})             // e.g: [[0 num] [1 str]]

		indexSpaceMap, exists := conn.packData.indexMap[spaceID]
		if !exists {
			indexSpaceMap = make(map[string]uint64)
			conn.packData.indexMap[spaceID] = indexSpaceMap
		}
		indexSpaceMap[indexName] = indexID

		// build list of primary key field numbers for this space, if the PK is detected
		if indexAttr != nil && indexID == 0 {
			if unique, ok := indexAttr["unique"]; ok && unique.(bool) {
				pk := make([]int, len(indexFields))
				for i := range indexFields {
					f := indexFields[i].([]interface{})
					pk[i] = int(f[0].(uint64))
				}
				conn.packData.primaryKeyMap[spaceID] = pk
			}
		}
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
		close(conn.writeChan)
		conn.tcpConn.Close()
		runtime.GC()
	})
}

func (conn *Connection) GetPrimaryKeyFields(space interface{}) ([]int, bool) {
	if conn.packData == nil {
		return nil, false
	}

	var spaceID uint64
	switch space.(type) {
	case int:
		spaceID = uint64(space.(int))
	case uint:
		spaceID = uint64(space.(uint))
	case uint32:
		spaceID = uint64(space.(uint32))
	case uint64:
		spaceID = space.(uint64)
	case string:
		spaceID = conn.packData.spaceMap[space.(string)]
	default:
		return nil, false
	}

	f, ok := conn.packData.primaryKeyMap[spaceID]
	return f, ok
}

func (conn *Connection) Close() {
	conn.stop()
	<-conn.closed
}

func (conn *Connection) String() string {
	return conn.remoteAddr
}

func (conn *Connection) IsClosed() (bool, error) {
	select {
	case <-conn.exit:
		return true, conn.getError()
	default:
		return false, conn.getError()
	}
}

func (conn *Connection) getError() error {
	conn.firstErrorLock.Lock()
	defer conn.firstErrorLock.Unlock()
	return conn.firstError
}

func (conn *Connection) setError(err error) {
	if err != nil && err != io.EOF {
		conn.firstErrorLock.Lock()
		if conn.firstError == nil {
			conn.firstError = err
		}
		conn.firstErrorLock.Unlock()
	}
}

func (conn *Connection) worker(tcpConn net.Conn) {

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		err := writer(tcpConn, conn.writeChan, conn.exit)
		conn.setError(err)
		conn.stop()
		wg.Done()
	}()

	go func() {
		err := conn.reader(tcpConn)
		conn.setError(err)
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

func writer(tcpConn net.Conn, writeChan chan *packedPacket, stopChan chan bool) (err error) {
	w := bufio.NewWriter(tcpConn)

WRITER_LOOP:
	for {
		select {
		case packet, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}

			_, err = packet.WriteTo(w)
			if err != nil {
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
			case packet, ok := <-writeChan:
				if !ok {
					break WRITER_LOOP
				}

				_, err = packet.WriteTo(w)
				if err != nil {
					break WRITER_LOOP
				}
			case <-stopChan:
				break WRITER_LOOP
			}
		}
	}

	return
}

func (conn *Connection) reader(tcpConn net.Conn) (err error) {
	var packet *Packet
	var pp *packedPacket
	var req *request

	r := bufio.NewReaderSize(tcpConn, 128*1024)

READER_LOOP:
	for {
		// read raw bytes
		pp, err = readPacked(r)
		if err != nil {
			break READER_LOOP
		}

		packet, err = decodePacket(pp)
		if err != nil {
			break READER_LOOP
		}

		req = conn.requests.Pop(packet.requestID)
		if req != nil {
			res := &Result{}

			if err != nil {
				res.Error = err
			} else if packet.result != nil {
				res = packet.result
			}

			req.replyChan <- res
			close(req.replyChan)
		}

		pp.Release()
		pp = nil
	}

	if pp != nil {
		pp.Release()
	}
	return
}
