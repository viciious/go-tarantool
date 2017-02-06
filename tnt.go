package tarantool

import "bytes"

type Query interface {
	Pack(requestID uint32, data *packData) ([]byte, error)
	Unpack(r *bytes.Buffer) error
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}
