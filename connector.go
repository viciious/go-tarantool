package tarantool

import (
	"sync"
)

type Connector struct {
	sync.Mutex
	RemoteAddr string
	options    *Options
	conn       *Connection
}

// New Connector instance.
func New(remoteAddr string, option *Options) *Connector {
	return &Connector{
		RemoteAddr: remoteAddr,
		options:    option,
	}
}

// Connect returns existing connection or will establish another one.
func (c *Connector) Connect() (*Connection, error) {
	c.Lock()
	defer c.Unlock()
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	return Connect(c.RemoteAddr, c.options)
}

// Close underlying connection.
func (c *Connector) Close() {
	c.Lock()
	defer c.Unlock()
	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}
	c.conn = nil
}
