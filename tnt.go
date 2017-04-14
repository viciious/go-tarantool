package tarantool

import "bytes"

var packetPool *packedPacketPool

type Query interface {
	Pack(data *packData, r *bytes.Buffer) (byte, error)
	Unpack(r *bytes.Buffer) error
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}

func init() {
	packetPool = NewPackedPacketPool()
}
