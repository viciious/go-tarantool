package tarantool

import (
	"context"
)

type ExecOption interface {
	apply(*request)
}

type opaqueOption struct {
	opaque interface{}
}

func (o *opaqueOption) apply(r *request) {
	r.opaque = o.opaque
}

func OpaqueExecOption(opaque interface{}) ExecOption {
	return &opaqueOption{opaque: opaque}
}

// the Result type is used to return write errors here
func (conn *Connection) writeRequest(ctx context.Context, request *request, q Query) (*request, *Result) {
	var err error

	requestID := conn.nextID()

	pp := packetPool.GetWithID(requestID)

	if err = pp.packMsg(q, conn.packData); err != nil {
		return nil, &Result{
			Error:     NewQueryError(ErrInvalidMsgpack, err.Error()),
			ErrorCode: ErrInvalidMsgpack,
		}
	}

	request.packet = pp

	if oldRequest := conn.requests.Put(requestID, request); oldRequest != nil {
		select {
		case oldRequest.replyChan <- &AsyncResult{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
			Opaque:    oldRequest.opaque,
		}:
		default:
		}
	}

	writeChan := conn.writeChan
	if writeChan == nil {
		return nil, &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}

	select {
	case writeChan <- request:
	case <-ctx.Done():
		if conn.perf.QueryTimeouts != nil && ctx.Err() == context.DeadlineExceeded {
			conn.perf.QueryTimeouts.Add(1)
		}
		r := conn.requests.Pop(requestID)
		requestPool.Put(r)
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

	return request, nil
}

func (conn *Connection) readResult(ctx context.Context, arc chan *AsyncResult) *AsyncResult {
	select {
	case ar := <-arc:
		if ar == nil {
			return &AsyncResult{
				Error:     ConnectionClosedError(conn),
				ErrorCode: ErrNoConnection,
			}
		}
		return ar
	case <-ctx.Done():
		if conn.perf.QueryTimeouts != nil && ctx.Err() == context.DeadlineExceeded {
			conn.perf.QueryTimeouts.Add(1)
		}
		return &AsyncResult{
			Error:     NewContextError(ctx, conn, "Recv error"),
			ErrorCode: ErrTimeout,
		}
	case <-conn.exit:
		return &AsyncResult{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}
}

func (conn *Connection) Exec(ctx context.Context, q Query, options ...ExecOption) (result *Result) {
	var cancel context.CancelFunc = func() {}

	if conn.queryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.queryTimeout)
	}

	replyChan := make(chan *AsyncResult, 1)

	request := requestPool.Get()
	request.replyChan = replyChan
	for i := 0; i < len(options); i++ {
		options[i].apply(request)
	}

	if _, rerr := conn.writeRequest(ctx, request, q); rerr != nil {
		cancel()
		return rerr
	}

	ar := conn.readResult(ctx, replyChan)
	cancel()

	if rerr := ar.Error; rerr != nil {
		return &Result{
			Error:     rerr,
			ErrorCode: ar.ErrorCode,
		}
	}

	pp := ar.BinaryPacket
	if pp == nil {
		return &Result{
			Error:     ConnectionClosedError(conn),
			ErrorCode: ErrNoConnection,
		}
	}

	if err := pp.Unmarshal(); err != nil {
		result = &Result{
			Error:     err,
			ErrorCode: ErrInvalidMsgpack,
		}
	} else {
		result = pp.Result()
		if result == nil {
			result = &Result{}
		}
	}
	pp.Release()

	return result
}

func (conn *Connection) ExecAsync(ctx context.Context, q Query, opaque interface{}, replyChan chan *AsyncResult) error {
	request := requestPool.Get()
	request.opaque = opaque
	request.replyChan = replyChan

	if _, rerr := conn.writeRequest(ctx, request, q); rerr != nil {
		return rerr.Error
	}
	return nil
}

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	res := conn.Exec(context.Background(), q)
	return res.Data, res.Error
}
