package tnt

import "time"

type QueryOptions struct {
	Timeout time.Duration
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) (result []interface{}, err error) {
	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}

	// make options
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.Timeout.Nanoseconds() == 0 {
		opts.Timeout = conn.queryTimeout
	}

	// set execute deadline
	deadline := time.After(opts.Timeout)

	select {
	case conn.requestChan <- request:
		// pass
	case <-deadline:
		return nil, NewConnectionError("Request send timeout")
	case <-conn.exit:
		return nil, ConnectionClosedError()
	}

	var response *Response
	select {
	case response = <-request.replyChan:
		// pass
	case <-deadline:
		return nil, NewConnectionError("Response read timeout")
	case <-conn.exit:
		return nil, ConnectionClosedError()
	}

	result = response.Data
	err = response.Error
	return
}

func (conn *Connection) Execute(q Query) (result []interface{}, err error) {
	return conn.ExecuteOptions(q, nil)
}
