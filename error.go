package tarantool

import (
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

type QueryError struct {
	error
}

var _ Error = (*ConnectionError)(nil)
var _ Error = (*QueryError)(nil)

func NewConnectionError(con *Connection, message string, timeout bool) error {
	return &ConnectionError{
		error: errors.New(fmt.Sprintf("%s, remote: %s", message, con.dsn.Host())),
	}
}

func ConnectionClosedError(con *Connection) error {
	return NewConnectionError(con, "Connection closed", false)
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
