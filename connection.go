package tarantool

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/phonkee/godsn"
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
	addr        string
	requestID   uint32
	requests    map[uint32]*request
	requestChan chan *request
	closeOnce   sync.Once
	exit        chan bool
	closed      chan bool
	tcpConn     net.Conn
	// options
	queryTimeout time.Duration
	Greeting     *Greeting
	packData     *packData
}

func Connect(addr string, options *Options) (conn *Connection, err error) {
	defer func() { // close opened connection if error
		if err != nil && conn != nil {
			if conn.tcpConn != nil {
				conn.tcpConn.Close()
			}
			conn = nil
		}
	}()

	conn = &Connection{
		addr:        addr,
		requests:    make(map[uint32]*request),
		requestChan: make(chan *request, 16),
		exit:        make(chan bool),
		closed:      make(chan bool),
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

	dsn, err := godsn.Parse("//" + addr)
	if err != nil {
		return nil, err
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
			splittedPath := strings.Split(path, "/")
			if len(splittedPath) > 1 {
				if splittedPath[1] == "" {
					return nil, fmt.Errorf("Wrong space: %s", splittedPath[1])
				}
				opts.DefaultSpace = splittedPath[1]
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

	conn.Greeting = &Greeting{
		Version: greeting[:64],
		Auth:    greeting[64:108],
	}

	if options.User != "" {
		var authRaw []byte
		var authResponse *Response

		authRequestID := conn.nextID()

		authRaw, err = (&Auth{
			User:         options.User,
			Password:     options.Password,
			GreetingAuth: conn.Greeting.Auth,
		}).Pack(authRequestID, conn.packData)

		_, err = conn.tcpConn.Write(authRaw)
		if err != nil {
			return
		}

		authResponse, err = read(conn.tcpConn)
		if err != nil {
			return
		}

		if authResponse.requestID != authRequestID {
			err = errors.New("Bad auth responseID")
			return
		}

		if authResponse.Error != nil {
			err = authResponse.Error
			return
		}
	}

	// select space and index schema
	request := func(req Query) (*Response, error) {
		var err error
		requestID := conn.nextID()
		packedReq, _ := (req).Pack(requestID, conn.packData)

		_, err = conn.tcpConn.Write(packedReq)
		if err != nil {
			return nil, err
		}

		res, err := read(conn.tcpConn)
		if err != nil {
			return nil, err
		}

		if res.requestID != requestID {
			return nil, errors.New("Bad auth responseID")
		}

		if res.Error != nil {
			return nil, res.Error
		}

		return res, nil
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
		conn.packData.spaceMap[space.([]interface{})[2].(string)] = space.([]interface{})[0].(uint64)
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
		spaceID := index.([]interface{})[0].(uint64)
		indexSpaceMap, exists := conn.packData.indexMap[spaceID]
		if !exists {
			indexSpaceMap = make(map[string]uint64)
			conn.packData.indexMap[spaceID] = indexSpaceMap
		}
		indexSpaceMap[index.([]interface{})[2].(string)] = index.([]interface{})[1].(uint64)
	}

	// remove deadline
	conn.tcpConn.SetDeadline(time.Time{})

	go conn.worker(conn.tcpConn)

	return
}

func (conn *Connection) nextID() uint32 {
	if conn.requestID == math.MaxUint32 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

func (conn *Connection) newRequest(r *request) error {
	requestID := conn.nextID()
	old, exists := conn.requests[requestID]
	if exists {
		old.replyChan <- &Response{
			Error: NewConnectionError("Shred old requests"), // wtf?
		}
		close(old.replyChan)
		delete(conn.requests, requestID)
	}

	// pp.Println(r)
	var err error

	r.raw, err = r.query.Pack(requestID, conn.packData)
	if err != nil {
		r.replyChan <- &Response{
			Error: &QueryError{
				error: err,
			},
		}
		return err
	}

	conn.requests[requestID] = r

	return nil
}

func (conn *Connection) handleReply(res *Response) {
	request, exists := conn.requests[res.requestID]
	if exists {
		request.replyChan <- res
		close(request.replyChan)
		delete(conn.requests, res.requestID)
	}
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

	readChan := make(chan *Response, 256)
	writeChan := make(chan *request, 256)

	wg.Add(3)

	go func() {
		conn.router(readChan, writeChan, conn.exit)
		conn.stop()
		wg.Done()
		// pp.Println("router")
	}()

	go func() {
		writer(tcpConn, writeChan, conn.exit)
		conn.stop()
		wg.Done()
		// pp.Println("writer")
	}()

	go func() {
		reader(tcpConn, readChan, conn.exit)
		conn.stop()
		wg.Done()
		// pp.Println("reader")
	}()

	wg.Wait()

	// send error reply to all pending requests
	for requestID, req := range conn.requests {
		req.replyChan <- &Response{
			Error: ConnectionClosedError(),
		}
		close(req.replyChan)
		delete(conn.requests, requestID)
	}

	var req *request

FETCH_INPUT:
	// and to all requests in input queue
	for {
		select {
		case req = <-conn.requestChan:
			// pass
		default: // all fetched
			break FETCH_INPUT
		}
		req.replyChan <- &Response{
			Error: ConnectionClosedError(),
		}
		close(req.replyChan)
	}

	close(conn.closed)
}

func (conn *Connection) router(readChan chan *Response, writeChan chan *request, stopChan chan bool) {
	// close(readChan) for stop router
	requestChan := conn.requestChan

	readChanThreshold := cap(readChan) / 10

ROUTER_LOOP:
	for {
		// force read reply
		if len(readChan) > readChanThreshold {
			requestChan = nil
		} else {
			requestChan = conn.requestChan
		}

		select {
		case r, ok := <-requestChan:
			if !ok {
				break ROUTER_LOOP
			}

			if conn.newRequest(r) == nil { // already replied to errored requests
				select {
				case writeChan <- r:
					// pass
				case <-stopChan:
					break ROUTER_LOOP
				}
			}
		case <-stopChan:
			break ROUTER_LOOP
		case res, ok := <-readChan:
			if !ok {
				break ROUTER_LOOP
			}
			conn.handleReply(res)
		}
	}
}

func writer(tcpConn net.Conn, writeChan chan *request, stopChan chan bool) {
	var err error
	var n int

	w := bufio.NewWriter(tcpConn)

WRITER_LOOP:
	for {
		select {
		case request, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			n, err = w.Write(request.raw)
			if err != nil || n != len(request.raw) {
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
			case request, ok := <-writeChan:
				if !ok {
					break WRITER_LOOP
				}
				n, err = w.Write(request.raw)
				if err != nil || n != len(request.raw) {
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

func reader(tcpConn net.Conn, readChan chan *Response, stopChan chan bool) {
	var response *Response
	var err error

	r := bufio.NewReaderSize(tcpConn, 128*1024)

READER_LOOP:
	for {
		response, err = read(r)
		if err != nil {
			break READER_LOOP
		}

		select {
		case readChan <- response:
			// pass
		case <-stopChan:
			break READER_LOOP
		}
	}
}

func packIproto(requestCode byte, requestID uint32, body []byte) []byte {
	h := [...]byte{
		0xce, 0, 0, 0, 0, // length
		0x82,                       // 2 element map
		KeyCode, byte(requestCode), // request code
		KeySync, 0xce,
		byte(requestID >> 24), byte(requestID >> 16),
		byte(requestID >> 8), byte(requestID),
	}

	l := uint32(len(h) - 5 + len(body))
	h[1] = byte(l >> 24)
	h[2] = byte(l >> 16)
	h[3] = byte(l >> 8)
	h[4] = byte(l)

	return append(h[:], body...)
}
