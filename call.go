package tarantool

import (
	"errors"

	"github.com/tinylib/msgp/msgp"
)

type Call struct {
	Name  string
	Tuple []interface{}
}

var _ Query = (*Call)(nil)

func (q *Call) GetCommandID() uint {
	return CallCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *Call) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 2)

	o = msgp.AppendUint(o, KeyFunctionName)
	o = msgp.AppendString(o, q.Name)

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
func (q *Call) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
	var t interface{}

	q.Name = ""
	q.Tuple = nil

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}
	if i != 2 {
		return buf, errors.New("Call.Unpack: expected map of length 2")
	}

	for ; i > 0; i-- {
		if k, buf, err = msgp.ReadUintBytes(buf); err != nil {
			return
		}

		switch k {
		case KeyFunctionName:
			if q.Name, buf, err = msgp.ReadStringBytes(buf); err != nil {
				return
			}
		case KeyTuple:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if err != nil {
				return buf, err
			}

			if q.Tuple = t.([]interface{}); q.Tuple == nil {
				return buf, errors.New("interface type is not []interface{}")
			}
			if len(q.Tuple) == 0 {
				q.Tuple = nil
			}
		}
	}

	if q.Name == "" {
		return buf, errors.New("Call.Unpack: no space specified")
	}

	return
}
