package tarantool

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"sync"
)

const saltSize = 32

type QueryHandler func(queryContext context.Context, query Query) *Result
type OnShutdownCallback func(err error)

type IprotoServer struct {
	sync.Mutex
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	uuid       string
	salt       []byte // base64-encoded salt
	ctx        context.Context
	cancel     context.CancelFunc
	handler    QueryHandler
	onShutdown OnShutdownCallback
	output     chan *BinaryPacket
	closeOnce  sync.Once
	firstError error
	perf       PerfCount
	schemaID   uint64
}

type IprotoServerOptions struct {
	Perf PerfCount
}

func NewIprotoServer(uuid string, handler QueryHandler, onShutdown OnShutdownCallback) *IprotoServer {
	return &IprotoServer{
		conn:       nil,
		reader:     nil,
		writer:     nil,
		handler:    handler,
		onShutdown: onShutdown,
		uuid:       uuid,
		schemaID:   1,
	}
}

func (s *IprotoServer) WithOptions(opts *IprotoServerOptions) *IprotoServer {
	if opts == nil {
		opts = &IprotoServerOptions{}
	}
	s.perf = opts.Perf
	return s
}

func (s *IprotoServer) Accept(conn net.Conn) {
	var ccr io.Reader
	var ccw io.Writer

	if s.perf.NetRead != nil {
		ccr = NewCountedReader(conn, s.perf.NetRead)
	} else {
		ccr = conn
	}

	if s.perf.NetWrite != nil {
		ccw = NewCountedWriter(conn, s.perf.NetWrite)
	} else {
		ccw = conn
	}

	s.conn = conn
	s.reader = bufio.NewReader(ccr)
	s.writer = bufio.NewWriter(ccw)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.output = make(chan *BinaryPacket, 1024)

	err := s.greet()
	if err != nil {
		s.Shutdown()
		return
	}

	go s.loop()
}

func (s *IprotoServer) CheckAuth(hash []byte, password string) bool {
	scr, err := scramble(s.salt, password)
	if err != nil {
		return false
	}

	if len(scr) != len(hash) {
		return false
	}

	for i, v := range hash {
		if v != scr[i] {
			return false
		}
	}
	return true
}

func (s *IprotoServer) setError(err error) {
	if err != nil && err != io.EOF {
		s.Lock()
		defer s.Unlock()
		if s.firstError == nil {
			s.firstError = err
		}
	}
}

func (s *IprotoServer) getError() error {
	s.Lock()
	defer s.Unlock()
	return s.firstError
}

func (s *IprotoServer) Shutdown() error {
	err := s.getError()

	s.closeOnce.Do(func() {
		s.cancel()
		if s.onShutdown != nil {
			s.onShutdown(err)
		}
		s.conn.Close()
	})

	return err
}

func (s *IprotoServer) greet() (err error) {
	var line1, line2 string
	var format, greeting string
	var n int

	salt := make([]byte, saltSize)
	_, err = rand.Read(salt)
	if err != nil {
		return
	}

	s.salt = []byte(base64.StdEncoding.EncodeToString(salt))

	line1 = fmt.Sprintf("%s %s", ServerIdent, s.uuid)
	line2 = string(s.salt)

	format = fmt.Sprintf("%%-%ds\n%%-%ds\n", GreetingSize/2-1, GreetingSize/2-1)
	greeting = fmt.Sprintf(format, line1, line2)

	// send greeting
	n, err = fmt.Fprintf(s.writer, "%s", greeting)
	if err != nil || n != GreetingSize {
		return
	}

	return s.writer.Flush()
}

func (s *IprotoServer) loop() {
	go s.read()
	go s.write()
}

func (s *IprotoServer) read() {
	var err error
	var pp *BinaryPacket

	r := s.reader

READER_LOOP:
	for {
		select {
		case <-s.ctx.Done():
			break READER_LOOP
		default:
			// read raw bytes
			pp = packetPool.Get()
			_, err = pp.ReadFrom(r)
			if err != nil {
				break READER_LOOP
			}

			if s.perf.NetPacketsIn != nil {
				s.perf.NetPacketsIn.Add(1)
			}

			go func(pp *BinaryPacket) {
				packet := &pp.packet

				err := packet.UnmarshalBinary(pp.body)

				if err != nil {
					s.setError(fmt.Errorf("Error decoding packet type %d: %s", packet.Cmd, err))
					s.Shutdown()
					return
				}

				code := packet.Cmd
				if code == PingCommand {
					pr := packetPool.GetWithID(packet.requestID)
					pr.packet.SchemaID = packet.SchemaID

					select {
					case s.output <- pr:
						break
					case <-s.ctx.Done():
						break
					}
				} else {
					res := s.handler(s.ctx, packet.Request)
					if res.ErrorCode != OKCommand && res.Error == nil {
						res.Error = ErrUnknownError
					}

					// reuse the same binary packet object for result marshalling
					if err = pp.packMsg(res, nil); err != nil {
						s.setError(err)
						s.Shutdown()
						return
					}

					pp.packet.SchemaID = s.schemaID
					select {
					case s.output <- pp:
						return
					case <-s.ctx.Done():
						break
					}
				}
				pp.Release()
			}(pp)
		}
	}

	if err != nil {
		s.setError(err)
	}
	s.Shutdown()

CLEANUP_LOOP:
	for {
		select {
		case pp = <-s.output:
			pp.Release()
		default:
			break CLEANUP_LOOP
		}
	}
}

func (s *IprotoServer) write() {
	var err error

	w := s.writer
	wp := func(w io.Writer, packet *BinaryPacket) error {
		if s.perf.NetPacketsOut != nil {
			s.perf.NetPacketsOut.Add(1)
		}
		_, err = packet.WriteTo(w)
		defer packet.Release()
		return err
	}

WRITER_LOOP:
	for {
		select {
		case packet, ok := <-s.output:
			if !ok {
				break WRITER_LOOP
			}
			if err = wp(w, packet); err != nil {
				break WRITER_LOOP
			}
		case <-s.ctx.Done():
			w.Flush()
			break WRITER_LOOP
		default:
			if err = w.Flush(); err != nil {
				break WRITER_LOOP
			}

			// same without flush
			select {
			case packet, ok := <-s.output:
				if !ok {
					break WRITER_LOOP
				}
				if err = wp(w, packet); err != nil {
					break WRITER_LOOP
				}
			case <-s.ctx.Done():
				w.Flush()
				break WRITER_LOOP
			}

		}
	}

	if err != nil {
		s.setError(err)
	}

	s.Shutdown()
}
