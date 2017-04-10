package tarantool

import (
	"bytes"
	"errors"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Replace struct {
	Space interface{}
	Tuple []interface{}
}

var _ Query = (*Replace)(nil)

func (s *Replace) Pack(data *packData, bodyBuffer *bytes.Buffer) (byte, error) {
	var err error

	if s.Tuple == nil {
		return BadRequest, errors.New("Tuple can not be nil")
	}

	encoder := msgpack.NewEncoder(bodyBuffer)

	encoder.EncodeMapLen(2) // Space, Tuple

	// Space
	if err = data.writeSpace(s.Space, bodyBuffer, encoder); err != nil {
		return BadRequest, err
	}

	// Tuple
	encoder.EncodeUint32(KeyTuple)
	encoder.EncodeArrayLen(len(s.Tuple))
	for _, value := range s.Tuple {
		if err = encoder.Encode(value); err != nil {
			return BadRequest, err
		}
	}

	return ReplaceRequest, nil
}

func (q *Replace) Unpack(r *bytes.Buffer) (err error) {
	var i int
	var k int
	var t uint

	q.Space = nil
	q.Tuple = nil

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
		case KeyTuple:
			q.Tuple, err = decoder.DecodeSlice()
			if err != nil {
				return err
			}
		}
	}

	if q.Space == nil {
		return errors.New("Replace.Unpack: no space specified")
	}
	if q.Tuple == nil {
		return errors.New("Replace.Unpack: no tuple specified")
	}

	return nil
}
