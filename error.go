package tarantool

import (
	"context"
	"errors"
	"fmt"
)

type Error interface {
	error
	Connection() bool // Is the error temporary?
}

type ConnectionError struct {
	error
	timeout bool
}

type ContextError struct {
	error
	contextErr error
}

type QueryError struct {
	error
}

var _ Error = (*ConnectionError)(nil)
var _ Error = (*QueryError)(nil)

var (
	// ErrNotInReplicaSet means there aren't full enough params to join replica set
	ErrNotInReplicaSet = NewQueryError("Full Replica Set params hasn't been set")
)

func NewConnectionError(con *Connection, message string, timeout bool) error {
	return &ConnectionError{
		error: fmt.Errorf("%s, remote: %s", message, con.dsn.Host()),
	}
}

func ConnectionClosedError(con *Connection) error {
	var message = "Connection closed"
	_, err := con.IsClosed()
	if err != nil {
		message = fmt.Sprintf("Connection error: %s", err)
	}
	return NewConnectionError(con, message, false)
}

func NewContextError(ctx context.Context, con *Connection, message string) error {
	return &ContextError{
		error:      fmt.Errorf("%s: %s, remote: %s", message, ctx.Err(), con.dsn.Host()),
		contextErr: ctx.Err(),
	}
}

func (e *ConnectionError) Connection() bool {
	return true
}

func (e *ConnectionError) Timeout() bool {
	return e.timeout
}

func (e *ConnectionError) Permanent() bool {
	return false
}

func NewQueryError(message string) error {
	return &QueryError{
		error: errors.New(message),
	}
}

func (e *QueryError) Connection() bool {
	return false
}

func (e *ContextError) Connection() bool {
	return false
}

func (e *ContextError) ContextErr() error {
	return e.contextErr
}
