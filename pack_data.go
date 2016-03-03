package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// cache precompiled
type packData struct {
	packedDefaultSpace  []byte
	packedDefaultIndex  []byte
	packedIterEq        []byte
	packedDefaultLimit  []byte
	packedDefaultOffset []byte
	packedSingleKey     []byte
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

func newPackData(defaultSpace interface{}) (*packData, error) {
	d := &packData{
		packedDefaultSpace:  encodeValues2(KeySpaceNo, defaultSpace),
		packedDefaultIndex:  encodeValues2(KeyIndexNo, uint32(0)),
		packedIterEq:        encodeValues2(KeyIterator, IterEq),
		packedDefaultLimit:  encodeValues2(KeyLimit, DefaultLimit),
		packedDefaultOffset: encodeValues2(KeyOffset, 0),
		packedSingleKey:     packSelectSingleKey(),
	}

	return d, nil
}

func testPackData() *packData {
	d, _ := newPackData(0)
	return d
}
