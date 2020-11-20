package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

type Upsert struct {
	Space interface{}
	Tuple []interface{}
	Set   []Operator
}

var _ Query = (*Upsert)(nil)

func (q *Upsert) GetCommandID() uint {
	return UpsertCommand
}

func (q *Upsert) packMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 3)

	if o, err = data.packSpace(q.Space, o); err != nil {
		return o, err
	}

	o = msgp.AppendUint(o, KeyTuple)
	if o, err = msgp.AppendIntf(o, q.Tuple); err != nil {
		return o, err
	}

	o = msgp.AppendUint(o, KeyDefTuple)
	o = msgp.AppendArrayHeader(o, uint32(len(q.Set)))
	for _, op := range q.Set {
		if o, err = msgp.AppendIntf(o, op.AsTuple()); err != nil {
			return o, err
		}
	}

	return o, nil
}

// MarshalMsg implements msgp.Marshaler
func (q *Upsert) MarshalMsg(b []byte) ([]byte, error) {
	return q.packMsg(defaultPackData, b)
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Upsert) UnmarshalMsg(data []byte) ([]byte, error) {
	return msgp.Skip(data)
}
