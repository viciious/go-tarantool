package tarantool

type Query interface {
	Pack(requestID uint32, data *packData) ([]byte, error)
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}
