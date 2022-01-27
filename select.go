package tarantool

import (
	"errors"

	"github.com/tinylib/msgp/msgp"
)

type Select struct {
	Space    interface{}
	Index    interface{}
	Offset   uint32
	Limit    uint32
	Iterator uint8
	Key      interface{}
	KeyTuple []interface{}
}

var _ Query = (*Select)(nil)

func (q *Select) GetCommandID() uint {
	return SelectCommand
}

func (q *Select) packMsg(data *packData, b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 6)

	if o, err = data.packSpace(q.Space, o); err != nil {
		return o, err
	}

	if o, err = data.packIndex(q.Space, q.Index, o); err != nil {
		return o, err
	}

	if q.Offset == 0 {
		o = append(o, data.packedDefaultOffset...)
	} else {
		o = msgp.AppendUint(o, KeyOffset)
		o = msgp.AppendUint(o, uint(q.Offset))
	}

	if q.Limit == 0 {
		o = append(o, data.packedDefaultLimit...)
	} else {
		o = msgp.AppendUint(o, KeyLimit)
		o = msgp.AppendUint(o, uint(q.Limit))
	}

	if q.Iterator == IterEq {
		o = append(o, data.packedIterEq...)
	} else {
		o = msgp.AppendUint(o, KeyIterator)
		o = msgp.AppendUint8(o, q.Iterator)
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
	} else {
		o = msgp.AppendUint(o, KeyKey)
		o = msgp.AppendArrayHeader(o, 0)
	}

	return o, nil
}

// MarshalMsg implements msgp.Marshaler
func (q *Select) MarshalMsg(b []byte) (data []byte, err error) {
	return q.packMsg(defaultPackData, b)
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *Select) UnmarshalMsg(data []byte) (buf []byte, err error) {
	var i uint32
	var k uint
	var t interface{}

	q.Space = nil
	q.Index = 0
	q.Offset = 0
	q.Limit = 0
	q.Iterator = IterEq

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
		case KeyOffset:
			if q.Offset, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		case KeyLimit:
			if q.Limit, buf, err = msgp.ReadUint32Bytes(buf); err != nil {
				return
			}
		case KeyIterator:
			if q.Iterator, buf, err = msgp.ReadUint8Bytes(buf); err != nil {
				return
			}
		case KeyKey:
			t, buf, err = msgp.ReadIntfBytes(buf)
			if err != nil {
				return buf, err
			}

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
		return buf, errors.New("Select.Unpack: no space specified")
	}

	return
}
