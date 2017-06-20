package tarantool

import (
	"bytes"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// cache precompiled
type packData struct {
	defaultSpace        interface{}
	packedDefaultSpace  []byte
	packedDefaultIndex  []byte
	packedIterEq        []byte
	packedDefaultLimit  []byte
	packedDefaultOffset []byte
	packedSingleKey     []byte
	spaceMap            map[string]uint64
	indexMap            map[uint64](map[string]uint64)
	primaryKeyMap       map[uint64]([]int)
}

func encodeValues2(v1, v2 interface{}) []byte {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.Encode(v1)
	encoder.Encode(v2)
	return buf.Bytes()
}

func packSelectSingleKey() []byte {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.EncodeUint32(KeyKey)
	encoder.EncodeArrayLen(1)
	return buf.Bytes()
}

func newPackData(defaultSpace interface{}) *packData {
	var packedDefaultSpace []byte
	if spaceNo, ok := defaultSpace.(uint64); ok {
		packedDefaultSpace = encodeValues2(KeySpaceNo, spaceNo)
	}
	return &packData{
		defaultSpace:        defaultSpace,
		packedDefaultSpace:  packedDefaultSpace,
		packedDefaultIndex:  encodeValues2(KeyIndexNo, uint32(0)),
		packedIterEq:        encodeValues2(KeyIterator, IterEq),
		packedDefaultLimit:  encodeValues2(KeyLimit, DefaultLimit),
		packedDefaultOffset: encodeValues2(KeyOffset, 0),
		packedSingleKey:     packSelectSingleKey(),
		spaceMap:            make(map[string]uint64),
		indexMap:            make(map[uint64](map[string]uint64)),
		primaryKeyMap:       make(map[uint64]([]int)),
	}
}

func (data *packData) spaceNo(space interface{}) (uint64, error) {
	if space == nil {
		space = data.defaultSpace
	}

	switch value := space.(type) {
	default:
		return 0, fmt.Errorf("Wrong space %#v", space)
	case int:
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case int64:
		return uint64(value), nil
	case uint64:
		return uint64(value), nil
	case int32:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case string:
		spaceNo, exists := data.spaceMap[value]
		if exists {
			return spaceNo, nil
		} else {
			return 0, fmt.Errorf("Unknown space %#v", space)
		}
	}
}

func (data *packData) encodeSpace(space interface{}, encoder *msgpack.Encoder) error {
	spaceNo, err := data.spaceNo(space)
	if err != nil {
		return err
	}

	encoder.EncodeUint32(KeySpaceNo)
	encoder.Encode(spaceNo)
	return nil
}

func (data *packData) writeSpace(space interface{}, w io.Writer, encoder *msgpack.Encoder) error {
	if space == nil && data.packedDefaultSpace != nil {
		w.Write(data.packedDefaultSpace)
		return nil
	}

	return data.encodeSpace(space, encoder)
}

func (data *packData) indexNo(space interface{}, index interface{}) (uint64, error) {
	if index == nil {
		return 0, nil
	}

	switch value := index.(type) {
	default:
		return 0, fmt.Errorf("Wrong index %#v", space)
	case int:
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case int64:
		return uint64(value), nil
	case uint64:
		return uint64(value), nil
	case int32:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case string:
		spaceNo, err := data.spaceNo(space)
		if err != nil {
			return 0, nil
		}

		spaceData, exists := data.indexMap[spaceNo]
		if !exists {
			return 0, fmt.Errorf("No indexes defined for space %#v", space)
		}

		indexNo, exists := spaceData[value]
		if exists {
			return indexNo, nil
		} else {
			return 0, fmt.Errorf("Unknown index %#v", index)
		}
	}
}

func (data *packData) writeIndex(space interface{}, index interface{}, w io.Writer, encoder *msgpack.Encoder) error {
	if index == nil {
		w.Write(data.packedDefaultIndex)
		return nil
	}

	indexNo, err := data.indexNo(space, index)
	if err != nil {
		return err
	}

	encoder.EncodeUint32(KeyIndexNo)
	encoder.Encode(indexNo)
	return nil
}
