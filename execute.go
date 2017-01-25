package tarantool

import (
	"context"
	"fmt"
	"time"
)

type QueryOptions struct {
	Timeout time.Duration
}

func (conn *Connection) doExecute(r *request, ctx context.Context) *Response {
	requestID := conn.nextID()

	packed, err := r.query.Pack(requestID, conn.packData)
	if err != nil {
		return &Response{
			Error: &QueryError{
				error: err,
			},
		}
	}

	oldRequest := conn.requests.Put(requestID, r)
	if oldRequest != nil {
		oldRequest.replyChan <- &Response{
			Error: NewConnectionError(conn, "Shred old requests", false), // wtf?
		}
		close(oldRequest.replyChan)
	}

	select {
	case conn.writeChan <- packed:
		// pass
	case <-ctx.Done():
		err := ctx.Err()
		return &Response{
			Error: NewConnectionError(conn, fmt.Sprintf("Send error: %s", err), err == context.DeadlineExceeded),
		}
	case <-conn.exit:
		return &Response{
			Error: ConnectionClosedError(conn),
		}
	}

	var response *Response
	select {
	case response = <-r.replyChan:
		// pass
	case <-ctx.Done():
		err := ctx.Err()
		return &Response{
			Error: NewConnectionError(conn, fmt.Sprintf("Recv error: %s", err), err == context.DeadlineExceeded),
		}
	case <-conn.exit:
		return &Response{
			Error: ConnectionClosedError(conn),
		}
	}

	return response
}

func (conn *Connection) Exec(q Query, ctx context.Context) *Result {
	var cancel context.CancelFunc = func() {}

	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}

	if _, ok := ctx.Deadline(); !ok && conn.queryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.queryTimeout)
	}
	response := conn.doExecute(request, ctx)
	cancel()

	return &Result{response.Error, response.Data}
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
