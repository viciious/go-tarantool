package tarantool

import (
	"io"

	"github.com/pkg/errors"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Join struct {
	UUID string
}

var _ Query = (*Join)(nil)

func (q *Join) Pack(data *packData, w io.Writer) (byte, error) {
	enc := msgpack.NewEncoder(w)

	enc.EncodeMapLen(1)
	enc.EncodeUint32(KeyInstanceUUID)
	enc.EncodeString(q.UUID)
	return JoinCommand, nil
}

func (q *Join) Unpack(r io.Reader) (err error) {
	// TODO: support Join Unpack
	return errors.New("Not supported yet")
}
