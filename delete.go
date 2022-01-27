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

func (q *Delete) GetCommandID() uint {
	return DeleteCommand
}

func (q *Delete) packMsg(data *packData, o []byte) ([]byte, error) {
	var err error

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

// MarshalMsg implements msgp.Marshaler
func (q *Delete) MarshalMsg(b []byte) (data []byte, err error) {
	return q.packMsg(defaultPackData, b)
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Delete) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
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
			if q.KeyTuple = t.([]interface{}); q.KeyTuple == nil {
				return buf, errors.New("interface type is not []interface{}")
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
