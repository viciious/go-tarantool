package tarantool

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"sync"
)

const greetingSize = 128
const saltSize = 32
const tarantoolVersion = "Tarantool 1.6.8 (Binary)"
const connBufSize = 2 * 1024 * 1024

type QueryHandler func(query Query) *Result
type OnCloseCallback func()

type IprotoServer struct {
	sync.Mutex
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	uuid      string
	salt      []byte // base64-encoded salt
	quit      chan bool
	handler   QueryHandler
	onClose   OnCloseCallback
	output    chan []byte
	closeOnce sync.Once
	lastError error
}

func NewIprotoServer(uuid string, handler QueryHandler, onClose OnCloseCallback) *IprotoServer {
	return &IprotoServer{
		conn:    nil,
		reader:  nil,
		writer:  nil,
		handler: handler,
		onClose: onClose,
		uuid:    uuid,
	}
}

func (s *IprotoServer) Accept(conn net.Conn) {
	s.conn = conn
	s.reader = bufio.NewReaderSize(conn, connBufSize)
	s.writer = bufio.NewWriterSize(conn, connBufSize)
	s.quit = make(chan bool)
	s.output = make(chan []byte, 1024)

	err := s.greet()
	if err != nil {
		s.Close()
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
		if s.lastError == nil {
			s.lastError = err
		}
	}
}

func (s *IprotoServer) Close() error {
	s.closeOnce.Do(func() {
		if s.onClose != nil {
			s.onClose()
		}
		close(s.quit)
		s.conn.Close()
	})

	s.Lock()
	defer s.Unlock()
	return s.lastError
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

	line1 = fmt.Sprintf("%s %s", tarantoolVersion, s.uuid)
	line2 = fmt.Sprintf("%s", s.salt)

	format = fmt.Sprintf("%%-%ds\n%%-%ds\n", greetingSize/2-1, greetingSize/2-1)
	greeting = fmt.Sprintf(format, line1, line2)

	// send greeting
	n, err = fmt.Fprintf(s.writer, "%s", greeting)
	if err != nil || n != greetingSize {
		return
	}

	return s.writer.Flush()
}

func (s *IprotoServer) loop() {
	go s.read()
	go s.write()
}

func (s *IprotoServer) read() {
	var packet *Packet
	var err error
	var body []byte

	r := s.reader

READER_LOOP:
	for {
		select {
		case <-s.quit:
			break READER_LOOP
		default:
			// read raw bytes
			body, err = readMessage(r)
			if err != nil {
				break READER_LOOP
			}

			packet, err = decodePacket(bytes.NewBuffer(body))
			if err != nil {
				break READER_LOOP
			}

			if packet.request != nil {
				go func(packet *Packet) {
					var res *Result
					var code = byte(packet.code)
					var body []byte

					if code == PingRequest {
						s.output <- packIprotoOk(packet.requestID)
					} else {
						res = s.handler(packet.request.(Query))
						body, _ = res.pack(packet.requestID)
						s.output <- body
					}
				}(packet)
			}
		}
	}

	if err != nil {
		s.setError(err)
	}
	s.Close()
}

func (s *IprotoServer) write() {
	var err error

	w := s.writer

WRITER_LOOP:
	for {
		select {
		case messageBody, ok := <-s.output:
			if !ok {
				break WRITER_LOOP
			}
			_, err = w.Write(messageBody)
			if err != nil {
				break WRITER_LOOP
			}
		case <-s.quit:
			w.Flush()
			break WRITER_LOOP
		default:
			if err = w.Flush(); err != nil {
				break WRITER_LOOP
			}

			// same without flush
			select {
			case messageBody, ok := <-s.output:
				if !ok {
					break WRITER_LOOP
				}
				_, err = w.Write(messageBody)
				if err != nil {
					break WRITER_LOOP
				}
			case <-s.quit:
				w.Flush()
				break WRITER_LOOP
			}

		}
	}

	if err != nil {
		s.setError(err)
	}

	s.Close()
}
