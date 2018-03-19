package tarantool

import (
	"github.com/tinylib/msgp/msgp"
)

type Update struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
	Set      []Operator
}

var _ Query = (*Update)(nil)

func (q Update) GetCommandID() int {
	return UpdateCommand
}

func (q Update) PackMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 4)

	if o, err = data.packSpace(q.Space, o); err != nil {
		return o, err
	}

	if o, err = data.packIndex(q.Space, q.Index, o); err != nil {
		return o, err
	}

	if q.Key != nil {
		o = append(o, data.packedSingleKey...)
		if o, err = msgp.AppendIntf(o, q.Key); err != nil {
			return o, err
		}
	} else if q.KeyTuple != nil {
		o = msgp.AppendUint(o, KeyKey)
		if o, err = msgp.AppendIntf(o, q.KeyTuple); err != nil {
			return o, err
		}
	}

	o = msgp.AppendUint(o, KeyTuple)
	o = msgp.AppendArrayHeader(o, uint32(len(q.Set)))
	for _, op := range q.Set {
		if o, err = msgp.AppendIntf(o, op.AsTuple()); err != nil {
			return o, err
		}
	}

	return o, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Update) UnmarshalBinary(data []byte) error {
	_, err := q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Update) UnmarshalMsg(data []byte) ([]byte, error) {
	return msgp.Skip(data)
}
