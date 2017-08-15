package tarantool

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
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

	enc.EncodeUint8(uint8(KeyInstanceUUID))
	enc.EncodeString(q.UUID)

	enc.EncodeUint8(uint8(KeyReplicaSetUUID))
	enc.EncodeString(q.ReplicaSetUUID)

	enc.EncodeUint8(uint8(KeyVClock))
	// NB: maybe we should omit zero element
	enc.EncodeMapLen(len(q.VClock))
	for id, lsn := range q.VClock {
		enc.EncodeUint32(uint32(id))
		enc.EncodeInt64(lsn)
	}

	return SubscribeRequest, nil
}

// Unpack implements a part of the Query interface
func (q *Subscribe) Unpack(r io.Reader) (err error) {
	return ErrNotSupported
}
