package tarantool

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"encoding/base64"
	"math/rand"
)

const greetingSize = 128
const saltSize = 32
const tarantoolVersion = "Tarantool 1.6.8 (Binary)"
const connBufSize = 128 * 1024

type IprotoServer struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	uuid   string
	salt   []byte
	quit   chan bool
}

func NewIprotoServer(uuid string) *IprotoServer {
	return &IprotoServer{
		conn:   nil,
		reader: nil,
		writer: nil,
		uuid:   uuid,
	}
}

func (s *IprotoServer) Accept(conn net.Conn) {
	s.conn = conn
	s.reader = bufio.NewReaderSize(conn, connBufSize)
	s.writer = bufio.NewWriterSize(conn, connBufSize)
	s.quit = make(chan bool)

	err := s.greet()
	if err != nil {
		conn.Close()
		return
	}

	s.loop()
}

func (s *IprotoServer) Close() {
	close(s.quit)
}

func (s *IprotoServer) greet() (err error) {
	var line1, line2 string
	var format, greeting string
	var n int

	s.salt = make([]byte, saltSize)
	_, err = rand.Read(s.salt)
	if err != nil {
		return
	}

	line1 = fmt.Sprintf("%s %s", tarantoolVersion, s.uuid)
	line2 = fmt.Sprintf("%s", base64.URLEncoding.EncodeToString(s.salt))

	format = fmt.Sprintf("%%-%ds\n%%-%ds\n", greetingSize/2 - 1, greetingSize/2 - 1)
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
	//var req *request

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

			fmt.Println("packet ", packet.Code)
		}
	}
}

func (s *IprotoServer) write() {
	for {
		select {
		case <-s.quit:
			return
		}
	}
}
