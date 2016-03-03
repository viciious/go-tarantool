package tnt

import (
	"net"
	"sync"
	"time"
)

type Query interface {
	Pack(requestID uint64, defaultSpace string) []byte
}

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	DefaultSpace   string
	User           string
	Password       string
}

type Response struct {
	// Data      []Tuple
	Code      uint64
	Error     error
	requestID uint64
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}

type Greeting struct {
	Version []byte
	Auth    []byte
}

type Connection struct {
	addr        string
	requestID   uint64
	requests    map[uint64]*request
	requestChan chan *request
	closeOnce   sync.Once
	exit        chan bool
	closed      chan bool
	tcpConn     net.Conn
	// options
	queryTimeout time.Duration
	defaultSpace string
	Greeting     *Greeting
}
