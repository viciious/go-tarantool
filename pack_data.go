package tarantool

import (
	"fmt"

	"github.com/tinylib/msgp/msgp"
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
	indexMap            map[uint64]map[string]uint64
	primaryKeyMap       map[uint64][]int
}

func encodeValues2(v1, v2 interface{}) []byte {
	o := make([]byte, 0)
	o, _ = msgp.AppendIntf(o, v1)
	o, _ = msgp.AppendIntf(o, v2)
	return o[:]
}

func packSelectSingleKey() []byte {
	o := make([]byte, 0)
	o = msgp.AppendUint(o, KeyKey)
	o = msgp.AppendArrayHeader(o, 1)
	return o[:]
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
		indexMap:            make(map[uint64]map[string]uint64),
		primaryKeyMap:       make(map[uint64][]int),
	}
}

func (data *packData) spaceNo(space interface{}) (uint64, error) {
	if space == nil {
		space = data.defaultSpace
	}

	switch value := space.(type) {
	case string:
		spaceNo, exists := data.spaceMap[value]
		if exists {
			return spaceNo, nil
		}
		return 0, fmt.Errorf("unknown space %#v", space)
	}

	return numberToUint64(space)
}

func (data *packData) packSpace(space interface{}, o []byte) ([]byte, error) {
	if space == nil && data.packedDefaultSpace != nil {
		o = append(o, data.packedDefaultSpace...)
		return o, nil
	}

	spaceNo, err := data.spaceNo(space)
	if err != nil {
		return o, err
	}

	o = msgp.AppendUint(o, KeySpaceNo)
	o = msgp.AppendUint64(o, spaceNo)
	return o, nil
}

func numberToUint64(number interface{}) (uint64, error) {
	switch value := number.(type) {
	default:
		return 0, fmt.Errorf("bad number %#v", number)
	case int:
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case int8:
		return uint64(value), nil
	case uint8:
		return uint64(value), nil
	case int16:
		return uint64(value), nil
	case uint16:
		return uint64(value), nil
	case int32:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case int64:
		return uint64(value), nil
	case uint64:
		return value, nil
	}
}

func (data *packData) fieldNo(field interface{}) (uint64, error) {
	return numberToUint64(field)
}

func (data *packData) indexNo(space interface{}, index interface{}) (uint64, error) {
	if index == nil {
		return 0, nil
	}

	if value, ok := index.(string); ok {
		spaceNo, err := data.spaceNo(space)
		if err != nil {
			return 0, nil
		}

		spaceData, exists := data.indexMap[spaceNo]
		if !exists {
			return 0, fmt.Errorf("no indexes defined for space %#v", space)
		}

		indexNo, exists := spaceData[value]
		if exists {
			return indexNo, nil
		}
		return 0, fmt.Errorf("unknown index %#v", index)
	}

	return numberToUint64(index)
}

func (data *packData) packIndex(space interface{}, index interface{}, o []byte) ([]byte, error) {
	if index == nil {
		o = append(o, data.packedDefaultIndex...)
		return o, nil
	}

	indexNo, err := data.indexNo(space, index)
	if err != nil {
		return o, err
	}

	o = msgp.AppendUint(o, KeyIndexNo)
	o = msgp.AppendUint64(o, indexNo)
	return o, nil
}
