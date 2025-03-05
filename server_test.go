package tarantool

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerPing(t *testing.T) {
	handler := func(queryContext context.Context, query Query) *Result {
		return &Result{}
	}

	s := NewIprotoServer("1", handler, nil)

	listenAddr := make(chan string)
	go func() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer ln.Close()

		listenAddr <- ln.Addr().String()
		close(listenAddr)

		conn, err := ln.Accept()
		require.NoError(t, err)

		s.Accept(conn)
	}()

	addr := <-listenAddr
	conn, err := Connect(addr, nil)
	require.NoError(t, err)

	res := conn.Exec(context.Background(), &Ping{})
	assert.Equal(t, res.ErrorCode, OKCommand)
	assert.NoError(t, res.Error)

	conn.Close()
	s.Shutdown()
}
