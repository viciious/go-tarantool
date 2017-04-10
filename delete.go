package tarantool

import (
	"bytes"
	"errors"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Delete struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
}

var _ Query = (*Delete)(nil)

func (s *Delete) Pack(data *packData, bodyBuffer *bytes.Buffer) (byte, error) {
	var err error

	encoder := msgpack.NewEncoder(bodyBuffer)

	encoder.EncodeMapLen(3) // Space, Index, Key

	// Space
	if err = data.writeSpace(s.Space, bodyBuffer, encoder); err != nil {
		return BadRequest, err
	}

	// Index
	if err = data.writeIndex(s.Space, s.Index, bodyBuffer, encoder); err != nil {
		return BadRequest, err
	}

	// Key
	if s.Key != nil {
		bodyBuffer.Write(data.packedSingleKey)
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
	}

	return DeleteRequest, nil
}

func (q *Delete) Unpack(r *bytes.Buffer) (err error) {
	var i int
	var k int
	var t uint

	q.Space = nil
	q.Index = 0
	q.Key = nil
	q.KeyTuple = nil

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
		return errors.New("Delete.Unpack: no space specified")
	}
	if q.Key == nil && q.KeyTuple == nil {
		return errors.New("Delete.Unpack: no tuple specified")
	}

	return nil
}
