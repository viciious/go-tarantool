package tarantool

import (
	"errors"

	"github.com/tinylib/msgp/msgp"
)

type Delete struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
}

var _ Query = (*Delete)(nil)

func (q Delete) GetCommandID() int {
	return DeleteCommand
}

func (q Delete) PackMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 3)

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

	return o, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Delete) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Delete) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k int
	var t interface{}

	q.Space = nil
	q.Index = 0
	q.Key = nil
	q.KeyTuple = nil

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
		case KeyIndexNo:
			if q.Index, buf, err = msgp.ReadUintBytes(buf); err != nil {
				return
			}
		case KeyKey:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if q.KeyTuple = t.([]interface{}); q.KeyTuple == nil {
				return buf, errors.New("Interface type is not []interface{}")
			}

			if len(q.KeyTuple) == 1 {
				q.Key = q.KeyTuple[0]
				q.KeyTuple = nil
			}
		}
	}

	if q.Space == nil {
		return buf, errors.New("Delete.Unpack: no space specified")
	}
	if q.Key == nil && q.KeyTuple == nil {
		return buf, errors.New("Delete.Unpack: no tuple specified")
	}

	return
}
