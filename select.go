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
	}

	return packIproto(SelectRequest, requestID, bodyBuffer.Bytes()), nil
}
