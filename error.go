package tnt

import "errors"

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

func NewConnectionError(message string) error {
	return &ConnectionError{
		error: errors.New(message),
	}
}

func ConnectionClosedError() error {
	return NewConnectionError("Connection closed")
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
