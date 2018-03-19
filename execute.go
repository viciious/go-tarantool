package tarantool

import (
	"context"
	"fmt"
)

func (conn *Connection) doExecute(ctx context.Context, r *request) (*BinaryPacket, *Result) {
	var err error

	requestID := conn.nextID()

	pp := packetPool.GetWithID(requestID)
	defer pp.Release()

	if err = pp.packMsg(r.query, conn.packData); err != nil {
		return nil, &Result{
			Error: &QueryError{
				Code:  ErrInvalidMsgpack,
				error: err,
			},
			ErrorCode: ErrInvalidMsgpack,
		}
	}

	if oldRequest := conn.requests.Put(requestID, r); oldRequest != nil {
		close(oldRequest.replyChan)
	}

	writeChan := conn.writeChan
	if writeChan == nil {
		return nil, &Result{
			Error: NewConnectionError(conn, "Connection closed"),
		}
	}

	select {
	case writeChan <- pp:
	case <-ctx.Done():
		conn.requests.Pop(requestID)
		return nil, &Result{
			Error:     NewContextError(ctx, conn, "Send error"),
			ErrorCode: ErrTimeout,
		}
	case <-conn.exit:
		return nil, &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}

	select {
	case pp := <-r.replyChan:
		return pp, nil
	case <-ctx.Done():
		return nil, &Result{
			Error:     NewContextError(ctx, conn, "Recv error"),
			ErrorCode: ErrTimeout,
		}
	case <-conn.exit:
		return nil, &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}
}

func (conn *Connection) Exec(ctx context.Context, q Query) *Result {
	var cancel context.CancelFunc = func() {}

	request := &request{
		query:     q,
		replyChan: make(chan *BinaryPacket, 1),
	}

	if _, ok := ctx.Deadline(); !ok && conn.queryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.queryTimeout)
	}

	pp, rerr := conn.doExecute(ctx, request)
	cancel()

	if rerr != nil {
		return rerr
	}

	var result *Result
	if err := pp.packet.UnmarshalBinary(pp.body); err != nil {
		result = &Result{Error: (fmt.Errorf("Error decoding packet type %d: %s", pp.packet.Cmd, err)), ErrorCode: ErrInvalidMsgpack}
	} else {
		result = pp.packet.Result
		if result == nil {
			result = &Result{}
		}
	}
	pp.Release()

	return result
}

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	res := conn.Exec(context.Background(), q)
	return res.Data, res.Error
}
