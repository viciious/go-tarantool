package tarantool

import (
	"context"
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

// Connect returns existing connection or will establish another one using the provided context.
func (c *Connector) ConnectContext(ctx context.Context) (conn *Connection, err error) {
	c.Lock()
	defer c.Unlock()

	if c.conn == nil || c.conn.IsClosed() {
		var dsn *url.URL
		dsn, c.options, err = parseOptions(c.RemoteAddr, c.options)
		if err != nil {
			return nil, err
		}
		// clear possible user:pass in order to log c.RemoteAddr securely
		c.RemoteAddr = dsn.Host
		c.conn, err = connect(ctx, dsn.Scheme, dsn.Host, c.options)
	}
	conn = c.conn

	return conn, err
}

// Connect returns existing connection or will establish another one.
func (c *Connector) Connect() (conn *Connection, err error) {
	return c.ConnectContext(context.Background())
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
