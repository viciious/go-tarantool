package tarantool

import (
	"bufio"
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
)

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	DefaultSpace   string
	User           string
	Password       string
	UUID           string
	ReplicaSetUUID string
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

// Connect to tarantool instance with options.
// Returned Connection could be used to execute queries.
func Connect(dsnString string, options *Options) (conn *Connection, err error) {
	// code below had appeared in result of splitting Connect into newConn and pullSchema
	// newConn should be refactored totally
	conn, err = newConn(dsnString, options)
	if err != nil {
		return
	}

	// set schema pulling deadline
	timeout := time.Duration(time.Second)
	if options != nil {
		if options.ConnectTimeout.Nanoseconds() != 0 {
			timeout = options.ConnectTimeout
		}
	}
	deadline := time.Now().Add(timeout)
	conn.tcpConn.SetDeadline(deadline)

	err = conn.pullSchema()
	if err != nil {
		conn.tcpConn.Close()
		conn = nil
		return
	}

	// remove deadline
	conn.tcpConn.SetDeadline(time.Time{})

	go conn.worker(conn.tcpConn)

	return
}

func newConn(dsnString string, options *Options) (conn *Connection, err error) {
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
		}).Pack(conn.packData, &pp.buffer)
		if err != nil {
			return
		}

		_, err = pp.WriteTo(conn.tcpConn)
		if err != nil {
			return
		}

		authResponse, err = readPacket(conn.tcpConn)
		if err != nil {
			return
		}

		if authResponse.requestID != requestID {
			err = errors.New("Bad auth responseID")
			return
		}

		if authResponse.Result != nil && authResponse.Result.Error != nil {
			err = authResponse.Result.Error
			return
		}
	}
	// remove deadline
	conn.tcpConn.SetDeadline(time.Time{})
	return
}

func (conn *Connection) pullSchema() (err error) {
	// select space and index schema
	request := func(q Query) (*Result, error) {
		var err error

		requestID := conn.nextID()

		pp := packIproto(0, requestID)
		defer pp.Release()

		pp.code, err = q.Pack(conn.packData, &pp.buffer)
		if err != nil {
			return nil, err
		}

		_, err = pp.WriteTo(conn.tcpConn)
		if err != nil {
			return nil, err
		}

		response, err := readPacket(conn.tcpConn)
		if err != nil {
			return nil, err
		}

		if response.requestID != requestID {
			return nil, errors.New("Bad response requestID")
		}

		if response.Result == nil {
			return nil, errors.New("Nil response result")
		}

		if response.Result.Error != nil {
			return nil, response.Result.Error
		}

		return response.Result, nil
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

	r := bufio.NewReaderSize(tcpConn, DefaultReaderBufSize)

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
			} else if packet.Result != nil {
				res = packet.Result
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

func readPacket(r io.Reader) (p *Packet, err error) {
	pp, err := readPacked(r)
	if err != nil {
		return nil, err
	}
	defer pp.Release()

	p, err = decodePacket(pp)
	if err != nil {
		return nil, err
	}

	return p, nil
}
