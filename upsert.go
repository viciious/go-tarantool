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

func (q Upsert) GetCommandID() uint {
	return UpsertCommand
}

func (q Upsert) PackMsg(data *packData, b []byte) (o []byte, err error) {
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

// MarshalBinary implements encoding.BinaryMarshaler
func (q *Upsert) MarshalBinary() (data []byte, err error) {
	return q.PackMsg(defaultPackData, nil)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Upsert) UnmarshalBinary(data []byte) error {
	_, err := q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Upsert) UnmarshalMsg(data []byte) ([]byte, error) {
	return msgp.Skip(data)
}
