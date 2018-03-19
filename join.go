package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

// Join is the JOIN command
type Join struct {
	UUID string
}

var _ Query = (*Join)(nil)

func (q Join) GetCommandID() int {
	return JoinCommand
}

func (q Join) PackMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 1)
	o = msgp.AppendUint(o, KeyInstanceUUID)
	o = msgp.AppendString(o, q.UUID)
	return o, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Join) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Join) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}
