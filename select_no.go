package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type SelectNo struct {
	SpaceNo  uint32
	IndexNo  uint32
	Offset   uint32
	Limit    uint32
	Iterator uint8
	Key      interface{}
}

var _ Query = (*SelectNo)(nil)

func (s *SelectNo) Pack(requestID uint32, defaultSpace string) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(6) // Space, Index, Offset, Limit, Iterator, Key

	// Space
	encoder.EncodeUint32(KeySpaceNo)
	encoder.EncodeUint32(s.SpaceNo)

	// Index
	encoder.EncodeUint32(KeyIndexNo)
	encoder.EncodeUint32(s.IndexNo)

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

	data := make(map[uint32]interface{})
	data[KeySpaceNo] = SpaceIndex
	data[KeyIndexNo] = 0
	data[KeyOffset] = 0
	data[KeyLimit] = 100
	data[KeyIterator] = IterGt
	data[KeyKey] = []interface{}{uint(0)}

	// body, err := msgpack.Marshal(data)
	// if err != nil {
	// return nil, err
	// }

	return packIproto(SelectRequest, requestID, bodyBuffer.Bytes()), nil
}
