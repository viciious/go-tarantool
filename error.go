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
}

type QueryError struct {
	error
}

var _ Error = (*ConnectionError)(nil)
var _ Error = (*QueryError)(nil)

func NewConnectionError(con *Connection, message string) error {
	return &ConnectionError{
		error: errors.New(fmt.Sprintf("%s, remote: %s", message, con.dsn.Host)),
	}
}

func ConnectionClosedError(con *Connection) error {
	return NewConnectionError(con, "Connection closed")
}

func (e *ConnectionError) Connection() bool {
	return true
}

func NewQueryError(message string) error {
	return &QueryError{
		error: errors.New(message),
	}
}

func (e *QueryError) Connection() bool {
	return false
}
