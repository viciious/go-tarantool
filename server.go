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
	output     chan *packedPacket
	closeOnce  sync.Once
	firstError error
}

func NewIprotoServer(uuid string, handler QueryHandler, onShutdown OnShutdownCallback) *IprotoServer {
	return &IprotoServer{
		conn:       nil,
		reader:     nil,
		writer:     nil,
		handler:    handler,
		onShutdown: onShutdown,
		uuid:       uuid,
	}
}

func (s *IprotoServer) Accept(conn net.Conn) {
	s.conn = conn
	s.reader = bufio.NewReader(conn)
	s.writer = bufio.NewWriter(conn)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.output = make(chan *packedPacket, 1024)

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
	line2 = fmt.Sprintf("%s", s.salt)

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
	var pp *packedPacket

	r := s.reader

READER_LOOP:
	for {
		select {
		case <-s.ctx.Done():
			break READER_LOOP
		default:
			// read raw bytes
			pp, err = readPacked(r)
			if err != nil {
				break READER_LOOP
			}

			go func(pp *packedPacket) {
				packet, err := decodePacket(pp)
				if err != nil {
					s.setError(err)
					s.Shutdown()
					return
				}

				code := byte(packet.code)
				if code == PingRequest {
					select {
					case s.output <- packIprotoOk(packet.requestID):
						break
					case <-s.ctx.Done():
						break
					}
				} else {
					res := s.handler(s.ctx, packet.Request)

					outBody, _ := res.pack(packet.requestID)
					select {
					case s.output <- outBody:
						break
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
	wp := func(w io.Writer, packet *packedPacket) error {
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

			err = wp(w, packet)
			if err != nil {
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
				err = wp(w, packet)
				if err != nil {
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
