package tarantool

import (
	"errors"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Subscribe is the SUBSCRIBE command
type Subscribe struct {
	UUID           string
	ReplicaSetUUID string
	LSN            int64
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
	enc.EncodeMapLen(1)
	// InstanceID must be always 1. I don't know why
	// if it is changed, master push all request (from lsn = 0)
	// see:
	// https://github.com/tarantool/tarantool/blob/1.6.9/src/box/relay.cc#L224-L246
	// https://github.com/tarantool/tarantool/blob/1.6.9/src/box/xrow.cc#L299-L366
	// https://github.com/tarantool/tarantool/blob/1.6.9/src/box/vclock.c#L38-L55
	enc.EncodeUint32(1)
	enc.EncodeInt64(q.LSN)

	return SubscribeRequest, nil
}

// Unpack implements a part of the Query interface
func (q *Subscribe) Unpack(r io.Reader) (err error) {
	// TODO: support Subscribe Unpack
	return errors.New("Not supported yet")
}
