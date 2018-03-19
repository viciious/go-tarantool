package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

// Subscribe is the SUBSCRIBE command
type Subscribe struct {
	UUID           string
	ReplicaSetUUID string
	VClock         VectorClock
}

var _ Query = (*Subscribe)(nil)

func (q Subscribe) GetCommandID() int {
	return SubscribeCommand
}

func (q Subscribe) PackMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 3)

	o = msgp.AppendUint(o, KeyInstanceUUID)
	o = msgp.AppendString(o, q.UUID)

	o = msgp.AppendUint(o, KeyReplicaSetUUID)
	o = msgp.AppendString(o, q.ReplicaSetUUID)

	o = msgp.AppendUint(o, KeyVClock)
	o = msgp.AppendMapHeader(o, uint32(len(q.VClock)))
	for id, lsn := range q.VClock {
		o = msgp.AppendUint(o, uint(id))
		o = msgp.AppendInt64(o, lsn)
	}

	return o, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Subscribe) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Subscribe) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}
