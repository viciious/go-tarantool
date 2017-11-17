package tarantool

import (
	"context"
)

func (conn *Connection) doExecute(ctx context.Context, r *request) *Result {
	var err error

	requestID := conn.nextID()

	pp := packIproto(0, requestID)
	defer pp.Release()

	if pp.code, err = r.query.Pack(conn.packData, &pp.buffer); err != nil {
		return &Result{
			Error: &QueryError{
				Code:  ErrInvalidMsgpack,
				error: err,
			},
			ErrorCode: ErrInvalidMsgpack,
		}
	}

	if oldRequest := conn.requests.Put(requestID, r); oldRequest != nil {
		oldRequest.replyChan <- &Result{
			Error: NewConnectionError(conn, "shred old requests"), // wtf?
		}
		close(oldRequest.replyChan)
	}

	writeChan := conn.writeChan
	if writeChan == nil {
		return &Result{
			Error: NewConnectionError(conn, "Connection closed"),
		}
	}

	select {
	case writeChan <- pp:
	case <-ctx.Done():
		conn.requests.Pop(requestID)
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

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	res := conn.Exec(context.Background(), q)
	return res.Data, res.Error
}
