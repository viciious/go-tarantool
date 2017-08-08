package tarantool

import (
	"io"

	"github.com/vmihailenco/msgpack"
)

// Join is the JOIN command
type Join struct {
	UUID string
}

var _ Query = (*Join)(nil)

// Pack implements a part of the Query interface
func (q *Join) Pack(data *packData, w io.Writer) (uint32, error) {
	enc := msgpack.NewEncoder(w)

	enc.EncodeMapLen(1)
	enc.EncodeUint(KeyInstanceUUID)
	enc.EncodeString(q.UUID)
	return JoinCommand, nil
}

// Unpack implements a part of the Query interface
func (q *Join) Unpack(r io.Reader) (err error) {
	return ErrNotSupported
}
