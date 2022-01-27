package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

// Subscribe is the SUBSCRIBE command
type Subscribe struct {
	UUID           string
	ReplicaSetUUID string
	VClock         VectorClock
	Anon           bool
}

var _ Query = (*Subscribe)(nil)

func (q *Subscribe) GetCommandID() uint {
	return SubscribeCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Subscribe) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	if q.Anon {
		o = msgp.AppendMapHeader(o, 4)

		o = msgp.AppendUint(o, KeyReplicaAnon)
		o = msgp.AppendBool(o, true)
	} else {
		o = msgp.AppendMapHeader(o, 3)
	}

	o = msgp.AppendUint(o, KeyInstanceUUID)
	o = msgp.AppendString(o, q.UUID)

	o = msgp.AppendUint(o, KeyReplicaSetUUID)
	o = msgp.AppendString(o, q.ReplicaSetUUID)

	o = msgp.AppendUint(o, KeyVClock)
	o = msgp.AppendMapHeader(o, uint32(len(q.VClock)))
	for id, lsn := range q.VClock {
		o = msgp.AppendUint(o, uint(id))
		o = msgp.AppendUint64(o, lsn)
	}

	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Subscribe) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}

type SubscribeResponse struct {
	ReplicaSetUUID string
	VClock         VectorClock
}

// UnmarshalMsg implements msgp.Unmarshaller
func (sr *SubscribeResponse) UnmarshalMsg(data []byte) (buf []byte, err error) {
	// skip binary header
	if buf, err = msgp.Skip(data); err != nil {
		return
	}

	// unmarshal body
	var count uint32

	if count, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; count > 0; count-- {
		var key uint

		if key, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}
		switch key {
		case KeyReplicaSetUUID:
			var str string

			if str, buf, err = msgp.ReadStringBytes(buf); err != nil {
				return
			}
			sr.ReplicaSetUUID = str
		case KeyVClock:
			var n uint32
			var id uint32
			var lsn uint64

			if n, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
				return
			}
			sr.VClock = NewVectorClock()
			for ; n > 0; n-- {
				if id, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
					return
				}
				if lsn, buf, err = msgp.ReadUint64Bytes(buf); err != nil {
					return
				}
				if !sr.VClock.Follow(id, lsn) {
					return buf, ErrVectorClock
				}
			}
		default:
			if buf, err = msgp.Skip(buf); err != nil {
				return
			}
		}
	}
	return
}
