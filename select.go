package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Select struct {
	Space    string
	Index    string
	Offset   uint32
	Limit    uint32
	Iterator uint8
	Key      []byte
}

var _ Query = (*Select)(nil)

func (s *Select) Pack(requestID uint32, defaultSpace string) ([]byte, error) {
	var bodyBuffer bytes.Buffer

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(6) // Space, Index, Offset, Limit, Iterator, Key

	// Space
	encoder.EncodeUint32(KeySpaceNo)
	if s.Space != "" {
		encoder.EncodeString(s.Space)
	} else {
		encoder.EncodeString(defaultSpace)
	}

	// Index
	encoder.EncodeUint32(KeyIndexNo)
	if s.Index != "" {
		encoder.EncodeString(s.Index)
	} else {
		encoder.EncodeString(DefaultIndex)
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
	encoder.EncodeBytes(s.Key)

	return packIproto(SelectRequest, requestID, bodyBuffer.Bytes()), nil
}
