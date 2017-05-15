package tarantool

import "io"

var packetPool *packedPacketPool

type Query interface {
	Pack(data *packData, w io.Writer) (uint32, error)
	Unpack(r io.Reader) error
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Result
}

// ReplicaSet is an alias for the map to store Instance IDs of the Replica Set
type ReplicaSet map[uint32]string

// VectorClock is an alias for the map to store Vector Clock
type VectorClock map[uint32]int64

func init() {
	packetPool = newPackedPacketPool()
}
