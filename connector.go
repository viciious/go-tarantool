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
	isClosed := c.conn == nil
	if c.conn != nil {
		isClosed, _ = c.conn.IsClosed()
	}
	if isClosed {
		c.conn, err = Connect(c.remoteAddr, c.options)
	}
	conn = c.conn
	c.Unlock()

	return conn, err
}

func (c *Connector) Close() {
	c.Lock()
	isClosed := c.conn == nil
	if c.conn != nil {
		isClosed, _ = c.conn.IsClosed()
	}
	if !isClosed {
		c.conn.Close()
	}
	c.conn = nil
	c.Unlock()
}
