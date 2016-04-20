package tarantool

import (
	"bytes"

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

func (s *Select) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(6) // Space, Index, Offset, Limit, Iterator, Key

	// Space
	if err = data.writeSpace(s.Space, bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Index
	if err = data.writeIndex(s.Space, s.Index, bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Offset
	if s.Offset == 0 {
		bodyBuffer.Write(data.packedDefaultOffset)
	} else {
		encoder.EncodeUint32(KeyOffset)
		encoder.EncodeUint32(s.Offset)
	}

	// Limit
	if s.Limit == 0 {
		bodyBuffer.Write(data.packedDefaultLimit)
	} else {
		encoder.EncodeUint32(KeyLimit)
		encoder.EncodeUint32(s.Limit)
	}

	// Iterator
	if s.Iterator == IterEq {
		bodyBuffer.Write(data.packedIterEq)
	} else {
		encoder.EncodeUint32(KeyIterator)
		encoder.EncodeUint8(s.Iterator)
	}

	// Key
	if s.Key != nil {
		bodyBuffer.Write(data.packedSingleKey)
		if err = encoder.Encode(s.Key); err != nil {
			return nil, err
		}
	} else if s.KeyTuple != nil {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(len(s.KeyTuple))
		for _, key := range s.KeyTuple {
			if err = encoder.Encode(key); err != nil {
				return nil, err
			}
		}
	} else {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(0)
	}

	return packIproto(SelectRequest, requestID, bodyBuffer.Bytes()), nil
}
