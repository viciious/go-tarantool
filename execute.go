package tarantool

import (
	"context"
	"fmt"
	"time"
)

type QueryOptions struct {
	Timeout time.Duration
}

func (conn *Connection) doExecute(r *request, ctx context.Context) *Result {
	requestID := conn.nextID()

	code, packed, err := r.query.Pack(conn.packData)
	if err != nil {
		return &Result{
			Error: &QueryError{
				error: err,
			},
		}
	}

	oldRequest := conn.requests.Put(requestID, r)
	if oldRequest != nil {
		oldRequest.replyChan <- &Result{
			Error: NewConnectionError(conn, "Shred old requests", false), // wtf?
		}
		close(oldRequest.replyChan)
	}

	select {
	case conn.writeChan <- packIproto(code, requestID, packed):
		// pass
	case <-ctx.Done():
		err := ctx.Err()
		return &Result{
			Error:     NewConnectionError(conn, fmt.Sprintf("Send error: %s", err), err == context.DeadlineExceeded),
			ErrorCode: ErrTimeout,
		}
	case <-conn.exit:
		return &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}

	var res *Result
	select {
	case res = <-r.replyChan:
		// pass
	case <-ctx.Done():
		err := ctx.Err()
		return &Result{
			Error:     NewConnectionError(conn, fmt.Sprintf("Recv error: %s", err), err == context.DeadlineExceeded),
			ErrorCode: ErrTimeout,
		}
	case <-conn.exit:
		return &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}

	return res
}

func (conn *Connection) Exec(q Query, ctx context.Context) *Result {
	var cancel context.CancelFunc = func() {}

	request := &request{
		query:     q,
		replyChan: make(chan *Result, 1),
	}

	if _, ok := ctx.Deadline(); !ok && conn.queryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.queryTimeout)
	}
	result := conn.doExecute(request, ctx)
	cancel()
	return result
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) ([][]interface{}, error) {
	var res *Result
	var ctx context.Context = context.Background()
	var cancel context.CancelFunc = func() {}

	// make options
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}

	res = conn.Exec(q, ctx)
	cancel()

	return res.Data, res.Error
}

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	return conn.ExecuteOptions(q, nil)
}
