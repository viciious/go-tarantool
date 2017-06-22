package tarantool

import (
	"context"
	"time"
)

type QueryOptions struct {
	Timeout time.Duration
}

func (conn *Connection) doExecute(ctx context.Context, r *request) *Result {
	var err error

	requestID := conn.nextID()

	pp := packIproto(0, requestID)
	defer pp.Release()

	pp.code, err = r.query.Pack(conn.packData, &pp.buffer)

	if err != nil {
		return &Result{
			Error: &QueryError{
				error: err,
			},
			ErrorCode: ErrInvalidMsgpack,
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
	case conn.writeChan <- pp:
		// pass
	case <-ctx.Done():
		return &Result{
			Error:     NewContextError(ctx, conn, "Send error"),
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
		return &Result{
			Error:     NewContextError(ctx, conn, "Recv error"),
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

func (conn *Connection) Exec(ctx context.Context, q Query) *Result {
	var cancel context.CancelFunc = func() {}

	request := &request{
		query:     q,
		replyChan: make(chan *Result, 1),
	}

	if _, ok := ctx.Deadline(); !ok && conn.queryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.queryTimeout)
	}
	result := conn.doExecute(ctx, request)
	cancel()
	return result
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) ([][]interface{}, error) {
	var res *Result
	var cancel context.CancelFunc = func() {}
	ctx := context.Background()

	// make options
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}

	res = conn.Exec(ctx, q)
	cancel()

	return res.Data, res.Error
}

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	return conn.ExecuteOptions(q, nil)
}
