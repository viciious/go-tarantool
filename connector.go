package tarantool

import (
	"net/url"
	"sync"
)

type Connector struct {
	sync.Mutex
	RemoteAddr string
	options    Options
	conn       *Connection
}

// New Connector instance.
func New(dsnString string, options *Options) *Connector {
	if options != nil {
		return &Connector{RemoteAddr: dsnString, options: *options}
	}
	return &Connector{RemoteAddr: dsnString}
}

// Connect returns existing connection or will establish another one.
func (c *Connector) Connect() (conn *Connection, err error) {
	c.Lock()
	defer c.Unlock()

	isClosed := c.conn == nil
	if c.conn != nil {
		isClosed, _ = c.conn.IsClosed()
	}
	if isClosed {
		var dsn *url.URL
		dsn, c.options, err = parseOptions(c.RemoteAddr, c.options)
		if err != nil {
			return nil, err
		}
		// clear possible user:pass in order to log c.RemoteAddr securely
		c.RemoteAddr = dsn.Host
		c.conn, err = connect(dsn.Scheme, dsn.Host, c.options)
	}
	conn = c.conn

	return conn, err
}

// Close underlying connection.
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
