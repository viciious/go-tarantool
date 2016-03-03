package tnt

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
}

var _ Query = (*Select)(nil)

func (s *Select) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(6) // Space, Index, Offset, Limit, Iterator, Key

	// Space
	if s.Space != nil {
		encoder.EncodeUint32(KeySpaceNo)
		encoder.Encode(s.Space)
	} else {
		bodyBuffer.Write(data.packedDefaultSpace)
	}

	// Index
	if s.Index != nil {
		encoder.EncodeUint32(KeyIndexNo)
		encoder.Encode(s.Index)
	} else {
		bodyBuffer.Write(data.packedDefaultIndex)
	}

	// Offset
	encoder.EncodeUint32(KeyOffset)
	encoder.EncodeUint32(s.Offset)

	// Limit
	encoder.EncodeUint32(KeyLimit)
	if s.Limit != 0 {
		encoder.EncodeUint32(s.Limit)
	} else {
		encoder.EncodeUint32(DefaultLimit)
	}

	// Iterator
	encoder.EncodeUint32(KeyIterator)
	encoder.EncodeUint8(s.Iterator)

	// Key
	encoder.EncodeUint32(KeyKey)
	encoder.EncodeArrayLen(1)
	if err = encoder.Encode(s.Key); err != nil {
		return nil, err
	}

	return packIproto(SelectRequest, requestID, bodyBuffer.Bytes()), nil
}
