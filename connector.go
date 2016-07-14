package tarantool

import (
	"sync"
)

type Connector struct {
	sync.Mutex
	remoteAddr string
	options    *Options
	conn       *Connection
}

func New(remoteAddr string, option *Options) *Connector {
	return &Connector{
		remoteAddr: remoteAddr,
		options:    option,
	}
}

func (c *Connector) Connect() (*Connection, error) {
	var err error
	var conn *Connection

	c.Lock()
	if c.conn == nil || c.conn.IsClosed() {
		c.conn, err = Connect(c.remoteAddr, c.options)
	}
	conn = c.conn
	c.Unlock()

	return conn, err
}
