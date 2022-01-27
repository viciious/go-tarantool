package tarantool

import (
	"errors"

	"github.com/tinylib/msgp/msgp"
)

// Eval query
type Eval struct {
	Expression string
	Tuple      []interface{}
}

var _ Query = (*Eval)(nil)

func (q *Eval) GetCommandID() uint {
	return EvalCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Eval) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 2)

	o = msgp.AppendUint(o, KeyExpression)
	o = msgp.AppendString(o, q.Expression)

	if q.Tuple == nil {
		o = msgp.AppendUint(o, KeyTuple)
		o = msgp.AppendArrayHeader(o, 0)
	} else {
		o = msgp.AppendUint(o, KeyTuple)
		if o, err = msgp.AppendIntf(o, q.Tuple); err != nil {
			return o, err
		}
	}

	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Eval) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
	var t interface{}

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	if i != 2 {
		return buf, errors.New("Eval.Unpack: expected map of length 2")
	}

	for ; i > 0; i-- {
		if k, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch k {
		case KeyExpression:
			if q.Expression, buf, err = msgp.ReadStringBytes(buf); err != nil {
				return
			}
		case KeyTuple:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if q.Tuple = t.([]interface{}); q.Tuple == nil {
				return buf, errors.New("interface type is not []interface{}")
			}
			if len(q.Tuple) == 0 {
				q.Tuple = nil
			}
		}
	}
	return
}
