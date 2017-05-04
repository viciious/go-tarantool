package tarantool

import "io"

var packetPool *packedPacketPool

type Query interface {
	Pack(data *packData, w io.Writer) (byte, error)
	Unpack(r io.Reader) error
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}

func init() {
	packetPool = newPackedPacketPool()
}
