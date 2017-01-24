package tarantool

import "time"

type QueryOptions struct {
	Timeout time.Duration
}

func (conn *Connection) doExecute(q Query, deadline <-chan time.Time, abort chan bool) ([][]interface{}, error) {
	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}

	requestID := conn.nextID()

	packed, err := request.query.Pack(requestID, conn.packData)
	if err != nil {
		request.replyChan <- &Response{
			Error: &QueryError{
				error: err,
			},
		}
		return nil, err
	}

	oldRequest := conn.requests.Put(requestID, request)
	if oldRequest != nil {
		oldRequest.replyChan <- &Response{
			Error: NewConnectionError(conn, "Shred old requests"), // wtf?
		}
		close(oldRequest.replyChan)
	}

	select {
	case conn.writeChan <- packed:
		// pass
	case <-deadline:
		return nil, NewConnectionError(conn, "Request send timeout")
	case <-abort:
		return nil, NewConnectionError(conn, "Request aborted")
	case <-conn.exit:
		return nil, ConnectionClosedError(conn)
	}

	var response *Response
	select {
	case response = <-request.replyChan:
		// pass
	case <-deadline:
		return nil, NewConnectionError(conn, "Response read timeout")
	case <-abort:
		return nil, NewConnectionError(conn, "Request aborted")
	case <-conn.exit:
		return nil, ConnectionClosedError(conn)
	}

	if response.Error == nil {
		// finish decode message body
		err = response.decodeBody(response.buf)
		if err != nil {
			response.Error = err
		}
	}

	return response.Data, response.Error
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) ([][]interface{}, error) {
	// make options
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.Timeout.Nanoseconds() == 0 {
		opts.Timeout = conn.queryTimeout
	}

	// set execute deadline
	deadline := time.After(opts.Timeout)

	return conn.doExecute(q, deadline, nil)
}

func (conn *Connection) Execute(q Query) ([][]interface{}, error) {
	return conn.ExecuteOptions(q, nil)
}
