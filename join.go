package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

// Join is the JOIN command
type Join struct {
	UUID string
}

var _ Query = (*Join)(nil)

func (q *Join) GetCommandID() uint {
	return JoinCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Join) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 1)
	o = msgp.AppendUint(o, KeyInstanceUUID)
	o = msgp.AppendString(o, q.UUID)
	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Join) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}
