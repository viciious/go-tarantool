package tarantool

import (
	"errors"

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
		if o, err = marshalOperator(op, o); err != nil {
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
func (q *Upsert) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
	var t interface{}

	q.Space = nil

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
		case KeyTuple:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if err != nil {
				return
			}

			if q.Tuple = t.([]interface{}); q.Tuple == nil {
				return buf, errors.New("interface type is not []interface{}")
			}
		case KeyDefTuple:
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
		return buf, errors.New("upsert.Unpack: no space specified")
	}

	return
}
