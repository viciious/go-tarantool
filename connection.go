package tnt

import (
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

func Connect(addr string, options *Options) (conn *Connection, err error) {
	conn = &Connection{
		addr:        addr,
		requests:    make(map[uint64]*request),
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

	splittedAddr := strings.Split(addr, "/")
	remoteAddr := splittedAddr[0]

	if opts.DefaultSpace == "" {
		if len(splittedAddr) > 1 {
			if splittedAddr[1] == "" {
				return nil, fmt.Errorf("Wrong space: %s", splittedAddr[1])
			}
			options.DefaultSpace = splittedAddr[1]
		}
	}

	conn.queryTimeout = opts.QueryTimeout
	conn.defaultSpace = opts.DefaultSpace

	conn.tcpConn, err = net.DialTimeout("tcp", remoteAddr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	greeting := make([]byte, 128)

	conn.tcpConn.SetDeadline(time.Now().Add(opts.ConnectTimeout))
	_, err = io.ReadFull(conn.tcpConn, greeting)
	if err != nil {
		conn.Close()
		return
	}
	conn.tcpConn.SetDeadline(time.Time{})

	conn.Greeting = &Greeting{
		Version: greeting[:64],
		Auth:    greeting[64:108],
	}

	go conn.worker(conn.tcpConn)

	return
}

func (conn *Connection) nextID() uint64 {
	if conn.requestID == math.MaxUint64 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

func (conn *Connection) newRequest(r *request) {
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
	r.raw = r.query.Pack(requestID, conn.defaultSpace)
	conn.requests[requestID] = r
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
		reader(tcpConn, readChan)
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

			conn.newRequest(r)

			select {
			case writeChan <- r:
				// pass
			case <-stopChan:
				break ROUTER_LOOP
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
WRITER_LOOP:
	for {
		select {
		case request, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			_, err = tcpConn.Write(request.raw)
			// @TODO: handle error
			if err != nil {
				break WRITER_LOOP
			}
		case <-stopChan:
			break WRITER_LOOP
		}
	}
	if err != nil {
		// @TODO
		// pp.Println(err)
	}
}

func reader(tcpConn net.Conn, readChan chan *Response) {
	// var msgLen uint32
	// var err error
	header := make([]byte, 12)
	headerLen := len(header)

	var bodyLen uint32
	var requestID uint64
	var response *Response

	var err error

READER_LOOP:
	for {
		_, err = io.ReadAtLeast(tcpConn, header, headerLen)
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}

		// bodyLen = UnpackInt(header[4:8])
		// requestID = UnpackInt(header[8:12])

		body := make([]byte, bodyLen)

		_, err = io.ReadAtLeast(tcpConn, body, int(bodyLen))
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}

		// response, err = UnpackBody(body)
		response = nil
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}
		response.requestID = requestID

		readChan <- response
	}
}

func (conn *Connection) Close() {
}
