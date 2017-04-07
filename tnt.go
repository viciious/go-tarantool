package tarantool

import "bytes"

var packetPool *bufferPool

type Query interface {
	Pack(data *packData) (byte, []byte, error)
	Unpack(r *bytes.Buffer) error
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}

func init() {
	packetPool = NewBufferPool()
}
