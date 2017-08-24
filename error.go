package tarantool

import (
	"context"
	"errors"
	"fmt"
)

type Error interface {
	error
	Temporary() bool // Connection true if the error is temporary
}

type ConnectionError struct {
	error
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
var _ Error = (*ContextError)(nil)

var (
	// ErrNotSupported is returned when an unimplemented query type or operation is encountered.
	ErrNotSupported = NewQueryError("not supported yet")
	// ErrNotInReplicaSet means that join operation can not be performed on a replica set due to missing parameters.
	ErrNotInReplicaSet = NewQueryError("Full Replica Set params hasn't been set")
	// ErrBadResult means that query result was of invalid type or length.
	ErrBadResult = NewQueryError("invalid result")
	// ErrVectorClock is returns in case of bad manipulation with vector clock.
	ErrVectorClock = NewQueryError("vclock manipulation")
	// ErrUnknown is returns when ErrorCode isn't OK but Error is nil in Result.
	ErrUnknownError = NewQueryError("unknown error")
)

func NewConnectionError(con *Connection, message string) *ConnectionError {
	return &ConnectionError{
		error: fmt.Errorf("%s, remote: %s", message, con.remoteAddr),
	}
}

func ConnectionClosedError(con *Connection) *ConnectionError {
	var message = "Connection closed"
	_, err := con.IsClosed()
	if err != nil {
		message = fmt.Sprintf("Connection error: %s", err)
	}
	return NewConnectionError(con, message)
}

func NewContextError(ctx context.Context, con *Connection, message string) *ContextError {
	return &ContextError{
		error:      fmt.Errorf("%s: %s, remote: %s", message, ctx.Err(), con.remoteAddr),
		contextErr: ctx.Err(),
	}
}

func (e *ConnectionError) Error() string {
	return e.error.Error()
}

func (e *ConnectionError) Temporary() bool {
	return true
}

func NewQueryError(message string) *QueryError {
	return &QueryError{
		error: errors.New(message),
	}
}

func (e *QueryError) Error() string {
	return e.error.Error()
}

func (e *QueryError) Temporary() bool {
	return false
}

func (e *ContextError) Temporary() bool {
	return true
}

func (e *ContextError) Error() string {
	return e.error.Error()
}

func (e *ContextError) ContextErr() error {
	return e.contextErr
}
