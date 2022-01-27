package tarantool

import (
	"errors"

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

func (q *Update) GetCommandID() uint {
	return UpdateCommand
}

func (q *Update) packMsg(data *packData, b []byte) (o []byte, err error) {
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
		if o, err = marshalOperator(op, o); err != nil {
			return o, err
		}
	}

	return o, nil
}

// MarshalMsg implements msgp.Marshaler
func (q *Update) MarshalMsg(b []byte) ([]byte, error) {
	return q.packMsg(defaultPackData, b)
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Update) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
	var t interface{}

	q.Space = nil
	q.Index = 0

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch k {
		case KeySpaceNo:
			if q.Space, buf, err = msgp.ReadUintBytes(buf); err != nil {
				return
			}
		case KeyIndexNo:
			if q.Index, buf, err = msgp.ReadUintBytes(buf); err != nil {
				return
			}
		case KeyKey:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if err != nil {
				return
			}

			if q.KeyTuple = t.([]interface{}); q.KeyTuple == nil {
				return buf, errors.New("interface type is not []interface{}")
			}

			if len(q.KeyTuple) == 1 {
				q.Key = q.KeyTuple[0]
				q.KeyTuple = nil
			}
		case KeyTuple:
			var len uint32
			if len, buf, err = msgp.ReadArrayHeaderBytes(buf); err != nil {
				return
			}

			q.Set = make([]Operator, len)
			for j := uint32(0); j < len; j++ {
				if q.Set[j], buf, err = unmarshalOperator(buf); err != nil {
					return
				}
			}
		}
	}

	if q.Space == nil {
		return buf, errors.New("upate.Unpack: no space specified")
	}

	return
}
