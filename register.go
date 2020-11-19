package tarantool

import "github.com/tinylib/msgp/msgp"

// Register is the REGISTER command
type Register struct {
	UUID   string
	VClock VectorClock
}

var _ Query = (*Register)(nil)

func (q *Register) GetCommandID() uint {
	return RegisterCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Register) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 2)

	o = msgp.AppendUint(o, KeyInstanceUUID)
	o = msgp.AppendString(o, q.UUID)

	o = msgp.AppendUint(o, KeyVClock)
	o = msgp.AppendMapHeader(o, uint32(len(q.VClock[1:])))

	for i, lsn := range q.VClock[1:] {
		o = msgp.AppendUint32(o, uint32(i))
		o = msgp.AppendUint64(o, lsn)
	}

	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Register) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}
