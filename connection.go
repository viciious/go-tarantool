package tarantool

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrEmptyDefaultSpace = errors.New("zero-length default space or unnecessary slash in dsn.path")
	ErrSyncFailed        = errors.New("SYNC failed")
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
	requestID uint64
	requests  *requestMap
	writeChan chan *BinaryPacket // packed messages with header
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

	dsn, opts, err := parseOptions(dsnString, options)
	if err != nil {
		return nil, err
	}

	conn, err = newConn(dsn.Scheme, dsn.Host, opts)
	if err != nil {
		return
	}

	// set schema pulling deadline
	deadline := time.Now().Add(opts.ConnectTimeout)
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

func newConn(scheme, addr string, opts Options) (conn *Connection, err error) {

	defer func() { // close opened connection if error
		if err != nil && conn != nil {
			if conn.tcpConn != nil {
				conn.tcpConn.Close()
			}
			conn = nil
		}
	}()

	conn = &Connection{
		remoteAddr:     addr,
		requests:       newRequestMap(),
		writeChan:      make(chan *BinaryPacket, 256),
		exit:           make(chan bool),
		closed:         make(chan bool),
		firstErrorLock: &sync.Mutex{},
		packData:       newPackData(opts.DefaultSpace),
		queryTimeout:   opts.QueryTimeout,
	}

	conn.tcpConn, err = net.DialTimeout(scheme, conn.remoteAddr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	greeting := make([]byte, 128)

	connectDeadline := time.Now().Add(opts.ConnectTimeout)
	conn.tcpConn.SetDeadline(connectDeadline)
	// removing deadline deferred
	defer conn.tcpConn.SetDeadline(time.Time{})

	_, err = io.ReadFull(conn.tcpConn, greeting)
	if err != nil {
		return
	}

	conn.Greeting = &Greeting{
		Version: greeting[:64],
		Auth:    greeting[64:108],
	}

	// try to authenticate if user have been provided
	if len(opts.User) > 0 {
		requestID := conn.nextID()

		pp := packetPool.GetWithID(requestID)

		err = pp.packMsg(&Auth{
			User:         opts.User,
			Password:     opts.Password,
			GreetingAuth: conn.Greeting.Auth,
		}, conn.packData)
		if err != nil {
			pp.Release()
			return
		}

		_, err = pp.WriteTo(conn.tcpConn)
		pp.Release()
		if err != nil {
			return
		}

		pp = packetPool.Get()
		defer pp.Release()

		if err = pp.readPacket(conn.tcpConn); err != nil {
			return
		}

		authResponse := &pp.packet
		if authResponse.requestID != requestID {
			err = ErrSyncFailed
			return
		}

		if authResponse.Result != nil && authResponse.Result.Error != nil {
			err = authResponse.Result.Error
			return
		}
	}

	return
}

func parseOptions(dsnString string, options *Options) (*url.URL, Options, error) {
	if options == nil {
		options = &Options{}
	}
	opts := *options // copy to new object

	// remove schema, if present

	// === for backward compatibility (only tcp despite of user wishes :)
	dsnString = strings.TrimPrefix(dsnString, "unix:")
	// ===

	// tcp is the default scheme
	switch {
	case strings.HasPrefix(dsnString, "tcp://"):
	case strings.HasPrefix(dsnString, "//"):
		dsnString = "tcp:" + dsnString
	default:
		dsnString = "tcp://" + dsnString
	}
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return dsn, opts, err
	}

	if opts.ConnectTimeout.Nanoseconds() == 0 {
		opts.ConnectTimeout = DefaultConnectTimeout
	}
	if opts.QueryTimeout.Nanoseconds() == 0 {
		opts.QueryTimeout = DefaultQueryTimeout
	}

	if len(opts.User) == 0 {
		if user := dsn.User; user != nil {
			opts.User = user.Username()
			opts.Password, _ = user.Password()
		}
	}

	if len(opts.DefaultSpace) == 0 && len(dsn.Path) > 0 {
		path := strings.TrimPrefix(dsn.Path, "/")
		// check it if it is necessary
		switch {
		case len(path) == 0:
			return nil, opts, ErrEmptyDefaultSpace
		//case strings.IndexAny(path, "/ ,") != -1:
		//	return nil, opts, ErrBadDSNPath
		default:
			opts.DefaultSpace = path
		}
	}

	return dsn, opts, nil
}

func (conn *Connection) pullSchema() (err error) {
	// select space and index schema
	request := func(q Query) (*Result, error) {
		var err error

		requestID := conn.nextID()

		pp := packetPool.GetWithID(requestID)
		if err = pp.packMsg(q, conn.packData); err != nil {
			pp.Release()
			return nil, err
		}

		_, err = pp.WriteTo(conn.tcpConn)
		pp.Release()
		if err != nil {
			return nil, err
		}

		pp = packetPool.Get()
		defer pp.Release()

		if err = pp.readPacket(conn.tcpConn); err != nil {
			return nil, err
		}

		response := &pp.packet
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
		spaceID, _ := conn.packData.spaceNo(space[0])
		conn.packData.spaceMap[space[2].(string)] = spaceID
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
		spaceID, _ := conn.packData.fieldNo(index[0])
		indexID, _ := conn.packData.fieldNo(index[1])
		indexName := index[2].(string)
		indexAttr := index[4].(map[string]interface{}) // e.g: {"unique": true}
		indexFields := index[5].([]interface{})        // e.g: [[0 num] [1 str]]

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
					descr := indexFields[i].([]interface{})
					f, _ := conn.packData.fieldNo(descr[0])
					pk[i] = int(f)
				}
				conn.packData.primaryKeyMap[spaceID] = pk
			}
		}
	}

	return
}

func (conn *Connection) nextID() uint64 {
	return atomic.AddUint64(&conn.requestID, 1)
}

func (conn *Connection) stop() {
	conn.closeOnce.Do(func() {
		// debug.PrintStack()
		close(conn.exit)
		conn.tcpConn.Close()
		runtime.GC()
	})
}

func (conn *Connection) GetPrimaryKeyFields(space interface{}) ([]int, bool) {
	var spaceID uint64
	var err error

	if conn.packData == nil {
		return nil, false
	}
	if spaceID, err = conn.packData.spaceNo(space); err != nil {
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

	// release all pending packets
	writeChan := conn.writeChan
	conn.writeChan = nil

CLEANUP_LOOP:
	for {
		select {
		case pp := <-writeChan:
			pp.Release()
		default:
			break CLEANUP_LOOP
		}
	}

	// send error reply to all pending requests
	conn.requests.CleanUp(func(req *request) {
		close(req.replyChan)
	})

	close(conn.closed)
}

func writer(tcpConn io.Writer, writeChan chan *BinaryPacket, stopChan chan bool) (err error) {
	w := bufio.NewWriterSize(tcpConn, DefaultWriterBufSize)

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

func (conn *Connection) reader(tcpConn io.Reader) (err error) {
	var pp *BinaryPacket
	var requestID uint64

	r := bufio.NewReaderSize(tcpConn, DefaultReaderBufSize)

READER_LOOP:
	for {
		pp := packetPool.Get()
		if requestID, err = pp.readRawPacket(r); err != nil {
			break READER_LOOP
		}

		req := conn.requests.Pop(requestID)
		if req == nil {
			pp.Release()
			pp = nil
			continue
		}

		select {
		case req.replyChan <- pp:
			break
		default:
			pp.Release()
		}
		close(req.replyChan)
		pp = nil
	}

	if pp != nil {
		pp.Release()
	}
	return
}
