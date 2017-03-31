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

func NewConnectionError(con *Connection, message string, timeout bool) error {
	return &ConnectionError{
		error: fmt.Errorf("%s, remote: %s", message, con.dsn.Host()),
	}
}

func ConnectionClosedError(con *Connection) error {
	var message string = "Connection closed"
	_, err := con.IsClosed()
	if err != nil {
		message = fmt.Sprintf("Connection error: %s", err)
	}
	return NewConnectionError(con, message, false)
}

func NewContextError(con *Connection, message string, ctx context.Context) error {
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
