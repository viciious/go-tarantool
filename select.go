package tarantool

import (
	"errors"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
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

func (s *Select) Pack(data *packData, w io.Writer) (byte, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(6) // Space, Index, Offset, Limit, Iterator, Key

	// Space
	if err = data.writeSpace(s.Space, w, encoder); err != nil {
		return BadRequest, err
	}

	// Index
	if err = data.writeIndex(s.Space, s.Index, w, encoder); err != nil {
		return BadRequest, err
	}

	// Offset
	if s.Offset == 0 {
		w.Write(data.packedDefaultOffset)
	} else {
		encoder.EncodeUint32(KeyOffset)
		encoder.EncodeUint32(s.Offset)
	}

	// Limit
	if s.Limit == 0 {
		w.Write(data.packedDefaultLimit)
	} else {
		encoder.EncodeUint32(KeyLimit)
		encoder.EncodeUint32(s.Limit)
	}

	// Iterator
	if s.Iterator == IterEq {
		w.Write(data.packedIterEq)
	} else {
		encoder.EncodeUint32(KeyIterator)
		encoder.EncodeUint8(s.Iterator)
	}

	// Key
	if s.Key != nil {
		w.Write(data.packedSingleKey)
		if err = encoder.Encode(s.Key); err != nil {
			return BadRequest, err
		}
	} else if s.KeyTuple != nil {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(len(s.KeyTuple))
		for _, key := range s.KeyTuple {
			if err = encoder.Encode(key); err != nil {
				return BadRequest, err
			}
		}
	} else {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(0)
	}

	return SelectRequest, nil
}

func (q *Select) Unpack(r io.Reader) (err error) {
	var i int
	var k int
	var t uint

	q.Space = nil
	q.Index = 0
	q.Offset = 0
	q.Limit = 0
	q.Iterator = IterEq

	decoder := msgpack.NewDecoder(r)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeInt(); err != nil {
			return
		}

		switch k {
		case KeySpaceNo:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Space = int(t)
		case KeyIndexNo:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Index = int(t)
		case KeyOffset:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Offset = uint32(t)
		case KeyLimit:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Limit = uint32(t)
		case KeyIterator:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Iterator = uint8(t)
		case KeyKey:
			if q.KeyTuple, err = decoder.DecodeSlice(); err != nil {
				return
			}
			if len(q.KeyTuple) == 1 {
				q.Key = q.KeyTuple[0]
				q.KeyTuple = nil
			}
		}
	}

	if q.Space == nil {
		return errors.New("Select.Unpack: no space specified")
	}

	return nil
}
