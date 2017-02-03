package tarantool

import "gopkg.in/vmihailenco/msgpack.v2"

type Query interface {
	Pack(requestID uint32, data *packData) ([]byte, error)
	Unpack(decoder *msgpack.Decoder) error
}

type Result struct {
	Error error
	Data  [][]interface{}
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}
