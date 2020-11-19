package tarantool

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrNotSupported is returned when an unimplemented query type or operation is encountered.
	ErrNotSupported = NewQueryError(ErrUnsupported, "not supported yet")
	// ErrNotInReplicaSet means that join operation can not be performed on a replica set due to missing parameters.
	ErrNotInReplicaSet = NewQueryError(0, "Full Replica Set params hasn't been set")
	// ErrBadResult means that query result was of invalid type or length.
	ErrBadResult = NewQueryError(0, "invalid result")
	// ErrVectorClock is returns in case of bad manipulation with vector clock.
	ErrVectorClock = NewQueryError(0, "vclock manipulation")
	// ErrUnknownError is returns when ErrorCode isn't OK but Error is nil in Result.
	ErrUnknownError = NewQueryError(ErrUnknown, "unknown error")
)

// Error has Temporary method which returns true if error is temporary.
// It is useful to quickly decide retry or not retry.
type Error interface {
	error
	Temporary() bool // Temporary true if the error is temporary
}

// ConnectionError is returned when something have been happened with connection.
type ConnectionError struct {
	error
}

// NewConnectionError returns ConnectionError with message and remoteAddr in error text.
func NewConnectionError(con *Connection, message string) *ConnectionError {
	return &ConnectionError{
		error: fmt.Errorf("%s, remote: %s", message, con.remoteAddr),
	}
}

// ConnectionClosedError returns ConnectionError with message about closed connection
// or error depending on the connection state. It is also has remoteAddr in error text.
func ConnectionClosedError(con *Connection) *ConnectionError {
	var message = "Connection closed"
	if err := con.getError(); err != nil {
		message = "Connection error: " + err.Error()
	}
	return NewConnectionError(con, message)
}

// Temporary implements Error interface.
func (e *ConnectionError) Temporary() bool {
	return true
}

// Timeout implements net.Error interface.
func (e *ConnectionError) Timeout() bool {
	return false
}

// ContextError is returned when request has been ended with context timeout or cancel.
type ContextError struct {
	error
	CtxErr error
}

// NewContextError returns ContextError with message and remoteAddr in error text.
// It is also has context error itself in CtxErr.
func NewContextError(ctx context.Context, con *Connection, message string) *ContextError {
	return &ContextError{
		error:  fmt.Errorf("%s: %s, remote: %s", message, ctx.Err(), con.remoteAddr),
		CtxErr: ctx.Err(),
	}
}

// Temporary implements Error interface.
func (e *ContextError) Temporary() bool {
	return true
}

// Timeout implements net.Error interface.
func (e *ContextError) Timeout() bool {
	return e.CtxErr == context.DeadlineExceeded
}

// QueryError is returned when query error has been happened.
// It has error Code.
type QueryError struct {
	error
	Code uint
}

// NewQueryError returns QueryError with message and Code.
func NewQueryError(code uint, message string) *QueryError {
	return &QueryError{
		Code:  code,
		error: errors.New(message),
	}
}

// Temporary implements Error interface.
func (e *QueryError) Temporary() bool {
	return false
}

// Timeout implements net.Error interface.
func (e *QueryError) Timeout() bool {
	return false
}

// UnexpectedReplicaSetUUIDError is returned when ReplicaSetUUID set in Options.ReplicaSetUUID is not equal to ReplicaSetUUID
// received during Join or JoinWithSnap. It is only an AnonSlave2 error!
type UnexpectedReplicaSetUUIDError struct {
	error
	Expected string
	Got      string
}

// NewUnexpectedReplicaSetUUIDError returns UnexpectedReplicaSetUUIDError.
func NewUnexpectedReplicaSetUUIDError(expected string, got string) *UnexpectedReplicaSetUUIDError {
	return &UnexpectedReplicaSetUUIDError{
		error:    errors.New(fmt.Sprintf("unexpected ReplicaSetUUID. Expected: %v. Got: %v", expected, got)),
		Expected: expected,
		Got:      got,
	}
}

// Temporary implements Error interface.
func (e *UnexpectedReplicaSetUUIDError) Temporary() bool {
	return false
}

// Timeout implements net.Error interface.
func (e *UnexpectedReplicaSetUUIDError) Timeout() bool {
	return false
}

var _ Error = (*ConnectionError)(nil)
var _ Error = (*QueryError)(nil)
var _ Error = (*ContextError)(nil)
var _ Error = (*UnexpectedReplicaSetUUIDError)(nil)
