package tarantool

import (
	"errors"

	"github.com/tinylib/msgp/msgp"
)

type Replace struct {
	Space interface{}
	Tuple []interface{}
}

var _ Query = (*Replace)(nil)

func (q Replace) PackMsg(data *packData, b []byte) (o []byte, code uint32, err error) {
	if q.Tuple == nil {
		return o, ErrorFlag, errors.New("Tuple can not be nil")
	}

	o = b
	o = msgp.AppendMapHeader(o, 2)

	if o, err = data.packSpace(q.Space, o); err != nil {
		return o, ErrorFlag, err
	}

	o = msgp.AppendUint(o, KeyTuple)
	if o, err = msgp.AppendIntf(o, q.Tuple); err != nil {
		return o, ErrorFlag, err
	}

	return o, ReplaceRequest, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Replace) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Replace) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k int
	var t interface{}

	q.Space = nil
	q.Tuple = nil

	buf = data
	if i, buf, err = msgp.ReadMapHeaderBytes(buf); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, buf, err = msgp.ReadIntBytes(buf); err != nil {
			return
		}

		switch k {
		case KeySpaceNo:
			if q.Space, buf, err = msgp.ReadIntBytes(buf); err != nil {
				return
			}
		case KeyTuple:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if q.Tuple = t.([]interface{}); q.Tuple == nil {
				return buf, errors.New("Interface type is not []interface{}")
			}
		}
	}

	if q.Space == nil {
		return buf, errors.New("Replace.Unpack: no space specified")
	}
	if q.Tuple == nil {
		return buf, errors.New("Replace.Unpack: no tuple specified")
	}

	return
}
