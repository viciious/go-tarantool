package tarantool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
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
	ErrInvalidGreeting   = errors.New("invalid greeting")
	ErrEmptyDefaultSpace = errors.New("zero-length default space or unnecessary slash in dsn.path")
	ErrSyncFailed        = errors.New("SYNC failed")

	versionPrefix = []byte("Tarantool ")
)

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	DefaultSpace   string
	User           string
	Password       string
	UUID           string
	ReplicaSetUUID string
	Perf           PerfCount

	// PoolMaxPacketSize describes maximum size of packet buffer
	// that can be added to packet pool.
	// If the packet size is 0, option is ignored.
	PoolMaxPacketSize int
}

type Greeting struct {
	Version uint32
	Auth    []byte
}

type Connection struct {
	requestID uint64
	requests  *requestMap
	writeChan chan *request // packed messages with header
	closeOnce sync.Once
	exit      chan bool
	closed    chan bool
	tcpConn   net.Conn

	ccr io.Reader
	ccw io.Writer

	// options
	queryTimeout      time.Duration
	greeting          *Greeting
	packData          *packData
	remoteAddr        string
	firstError        error
	firstErrorLock    *sync.Mutex
	perf              PerfCount
	poolMaxPacketSize int
}

// Connect to tarantool instance with options using the provided context.
// Returned Connection can be used to execute queries.
func ConnectContext(ctx context.Context, dsnString string, options *Options) (conn *Connection, err error) {
	var opts Options
	if options != nil {
		opts = *options
	}
	dsn, opts, err := parseOptions(dsnString, opts)
	if err != nil {
		return nil, err
	}
	return connect(ctx, dsn.Scheme, dsn.Host, opts)
}

// Connect to tarantool instance with options
func Connect(dsnString string, options *Options) (conn *Connection, err error) {
	return ConnectContext(context.Background(), dsnString, options)
}

func connect(ctx context.Context, scheme, addr string, opts Options) (conn *Connection, err error) {
	conn, err = newConn(ctx, scheme, addr, opts)
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

	go conn.worker()

	return
}

func newConn(ctx context.Context, scheme, addr string, opts Options) (conn *Connection, err error) {
	defer func() { // close opened connection if error
		if err != nil && conn != nil {
			if conn.tcpConn != nil {
				conn.tcpConn.Close()
			}
			conn = nil
		}
	}()

	conn = &Connection{
		remoteAddr:        addr,
		requests:          newRequestMap(),
		writeChan:         make(chan *request, 256),
		exit:              make(chan bool),
		closed:            make(chan bool),
		firstErrorLock:    &sync.Mutex{},
		packData:          newPackData(opts.DefaultSpace),
		queryTimeout:      opts.QueryTimeout,
		perf:              opts.Perf,
		poolMaxPacketSize: opts.PoolMaxPacketSize,
	}

	d := &net.Dialer{
		Timeout: opts.ConnectTimeout,
	}

	conn.tcpConn, err = d.DialContext(ctx, scheme, conn.remoteAddr)
	if err != nil {
		return nil, err
	}

	if conn.perf.NetRead != nil {
		conn.ccr = NewCountedReader(conn.tcpConn, conn.perf.NetRead)
	} else {
		conn.ccr = conn.tcpConn
	}

	if conn.perf.NetWrite != nil {
		conn.ccw = NewCountedWriter(conn.tcpConn, conn.perf.NetWrite)
	} else {
		conn.ccw = conn.tcpConn
	}

	connectDeadline := time.Now().Add(opts.ConnectTimeout)
	conn.tcpConn.SetDeadline(connectDeadline)
	// removing deadline deferred
	defer conn.tcpConn.SetDeadline(time.Time{})

	if conn.greeting, err = parseGreeting(conn.ccr); err != nil {
		return
	}

	// try to authenticate if user have been provided
	if len(opts.User) > 0 {
		requestID := conn.nextID()

		pp := packetPool.GetWithID(requestID)

		err = pp.packMsg(&Auth{
			User:         opts.User,
			Password:     opts.Password,
			GreetingAuth: conn.greeting.Auth,
		}, conn.packData)
		if err != nil {
			conn.releasePacket(pp)
			return
		}

		_, err = pp.WriteTo(conn.ccw)
		conn.releasePacket(pp)
		if err != nil {
			return
		}

		pp = packetPool.Get()
		defer conn.releasePacket(pp)

		if err = pp.readPacket(conn.ccr); err != nil {
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

func parseOptions(dsnString string, opts Options) (*url.URL, Options, error) {
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

	if opts.ConnectTimeout.Nanoseconds() == 0 {
		opts.ConnectTimeout = DefaultConnectTimeout
	}
	if opts.QueryTimeout.Nanoseconds() == 0 {
		opts.QueryTimeout = DefaultQueryTimeout
	}

	return dsn, opts, nil
}

func parseGreeting(r io.Reader) (*Greeting, error) {
	greeting := make([]byte, 128)

	_, err := io.ReadFull(r, greeting)
	if err != nil {
		return nil, err
	}

	version, err := parseVersion(greeting[:64])
	if err != nil {
		return nil, err
	}

	return &Greeting{
		Version: version,
		Auth:    greeting[64:108],
	}, nil
}

func parseVersion(version []byte) (uint32, error) {
	if !bytes.HasPrefix(version, versionPrefix) {
		return 0, ErrInvalidGreeting
	}

	var major, minor, patch uint32
	str := string(version[len(versionPrefix):])
	if n, _ := fmt.Sscanf(str, "%d.%d.%d", &major, &minor, &patch); n != 3 {
		return 0, ErrInvalidGreeting
	}

	return VersionID(major, minor, patch), nil
}

func VersionID(major, minor, patch uint32) uint32 {
	return (((major << 8) | minor) << 8) | patch
}

func (conn *Connection) pullSchema() (err error) {
	// select space and index schema
	request := func(q Query) (*Result, error) {
		var err error

		requestID := conn.nextID()

		pp := packetPool.GetWithID(requestID)
		if err = pp.packMsg(q, conn.packData); err != nil {
			conn.releasePacket(pp)
			return nil, err
		}

		_, err = pp.WriteTo(conn.ccw)
		conn.releasePacket(pp)
		if err != nil {
			return nil, err
		}

		pp = packetPool.Get()
		defer conn.releasePacket(pp)

		if err = pp.readPacket(conn.ccr); err != nil {
			return nil, err
		}

		response := &pp.packet
		if response.requestID != requestID {
			return nil, errors.New("bad response requestID")
		}

		if response.Result == nil {
			return nil, errors.New("nil response result")
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
					switch descr := indexFields[i].(type) {
					case []interface{}:
						f, _ := conn.packData.fieldNo(descr[0])
						pk[i] = int(f)
					case map[string]interface{}:
						f, _ := conn.packData.fieldNo(descr["field"])
						pk[i] = int(f)
					default:
						panic("invalid index field format")
					}
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

func (conn *Connection) GetPerf() PerfCount {
	return conn.perf
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

func (conn *Connection) IsClosed() bool {
	select {
	case <-conn.exit:
		return true
	default:
		return false
	}
}

func (conn *Connection) releasePacket(pp *BinaryPacket) {
	if conn.poolMaxPacketSize == 0 || conn.poolMaxPacketSize < cap(pp.body) {
		pp.Release()
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

func (conn *Connection) worker() {
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		err := conn.writer()
		conn.setError(err)
		conn.stop()
		wg.Done()
	}()

	go func() {
		err := conn.reader()
		conn.setError(err)
		conn.stop()
		wg.Done()
	}()

	wg.Wait()

	// release all pending packets
	writeChan := conn.writeChan

CLEANUP_LOOP:
	for {
		select {
		case req := <-writeChan:
			pp := req.packet
			if pp != nil {
				req.packet = nil
				conn.releasePacket(pp)
			}
		default:
			break CLEANUP_LOOP
		}
	}

	// send error reply to all pending requests
	conn.requests.CleanUp(func(req *request) {
		select {
		case req.replyChan <- &AsyncResult{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
			Opaque:    req.opaque,
		}:
		default:
		}
		requestPool.Put(req)
	})

	close(conn.closed)
}

func (conn *Connection) writer() (err error) {
	writeChan := conn.writeChan
	stopChan := conn.exit
	w := bufio.NewWriterSize(conn.ccw, DefaultWriterBufSize)

	wr := func(w io.Writer, req *request) error {
		packet := req.packet

		if conn.perf.NetPacketsOut != nil {
			conn.perf.NetPacketsOut.Add(1)
		}
		if conn.perf.QueryComplete != nil && req.opaque != nil {
			req.startedAt = time.Now()
		}

		_, err := packet.WriteTo(w)
		req.packet = nil
		conn.releasePacket(packet)
		return err
	}

WRITER_LOOP:
	for {
		select {
		case req, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			if err = wr(w, req); err != nil {
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
			case req, ok := <-writeChan:
				if !ok {
					break WRITER_LOOP
				}
				if err = wr(w, req); err != nil {
					break WRITER_LOOP
				}
			case <-stopChan:
				break WRITER_LOOP
			}
		}
	}

	return
}

func (conn *Connection) reader() (err error) {
	var pp *BinaryPacket
	var requestID uint64

	r := bufio.NewReaderSize(conn.ccr, DefaultReaderBufSize)

READER_LOOP:
	for {
		pp := packetPool.Get()
		if requestID, err = pp.readRawPacket(r); err != nil {
			break READER_LOOP
		}

		if conn.perf.NetPacketsIn != nil {
			conn.perf.NetPacketsIn.Add(1)
		}

		req := conn.requests.Pop(requestID)
		if req == nil {
			conn.releasePacket(pp)
			pp = nil
			continue
		}

		if conn.perf.QueryComplete != nil && req.opaque != nil {
			conn.perf.QueryComplete(req.opaque, time.Since(req.startedAt))
		}

		select {
		case req.replyChan <- &AsyncResult{0, nil, pp, conn, req.opaque}:
			pp = nil
		default:
		}

		requestPool.Put(req)
	}

	if pp != nil {
		conn.releasePacket(pp)
	}
	return
}
