package tarantool

import (
	"io"

	"github.com/vmihailenco/msgpack"
)

// Subscribe is the SUBSCRIBE command
type Subscribe struct {
	UUID           string
	ReplicaSetUUID string
	VClock         VectorClock
}

var _ Query = (*Subscribe)(nil)

// Pack implements a part of the Query interface
func (q *Subscribe) Pack(data *packData, w io.Writer) (uint32, error) {
	enc := msgpack.NewEncoder(w)

	enc.EncodeMapLen(3)

	enc.EncodeUint(KeyInstanceUUID)
	enc.EncodeString(q.UUID)

	enc.EncodeUint(KeyReplicaSetUUID)
	enc.EncodeString(q.ReplicaSetUUID)

	enc.EncodeUint(KeyVClock)
	// NB: maybe we should omit zero element
	enc.EncodeMapLen(len(q.VClock))
	for id, lsn := range q.VClock {
		enc.EncodeUint(uint64(id))
		enc.EncodeInt(lsn)
	}

	return SubscribeRequest, nil
}

// Unpack implements a part of the Query interface
func (q *Subscribe) Unpack(r io.Reader) (err error) {
	return ErrNotSupported
}
