package tarantool

import (
	"context"
	"net/url"
)

type Connector struct {
	RemoteAddr string
	options    Options
	conn       *Connection
	ch         chan struct{}
}

// New Connector instance.
func New(dsnString string, options *Options) *Connector {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	if options != nil {
		return &Connector{RemoteAddr: dsnString, options: *options, ch: ch}
	}
	return &Connector{RemoteAddr: dsnString, ch: ch}
}

// Connect returns existing connection or will establish another one.
func (c *Connector) Connect() (conn *Connection, err error) {
	return c.ConnectContext(context.Background())
}

// Connect returns existing connection or will establish another one.
func (c *Connector) ConnectContext(ctx context.Context) (conn *Connection, err error) {
	doneChan := ctx.Done()
	if doneChan != nil {
		select {
		case <-doneChan:
			return nil, ctx.Err()
		case <-c.ch:
		}
	} else {
		<- c.ch
	}

	if c.conn == nil || c.conn.IsClosed() {
		var dsn *url.URL
		dsn, c.options, err = parseOptions(c.RemoteAddr, c.options)
		if err != nil {
			c.ch <- struct {}{}
			return nil, err
		}
		// clear possible user:pass in order to log c.RemoteAddr securely
		c.RemoteAddr = dsn.Host
		c.conn, err = connect(ctx, dsn.Scheme, dsn.Host, c.options)
	}
	conn = c.conn
	c.ch <- struct {}{}

	return conn, err
}

// Close underlying connection.
func (c *Connector) Close() {
	<- c.ch
	if c.conn != nil && !c.conn.IsClosed() {
		c.conn.Close()
	}
	c.conn = nil
	c.ch <- struct {}{}
}
