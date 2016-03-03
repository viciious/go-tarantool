package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// cache precompiled
type packData struct {
	packedDefaultSpace []byte
}

func encodeValues2(v1, v2 interface{}) []byte {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.Encode(v1)
	encoder.Encode(v2)
	return buf.Bytes()
}

func newPackData(defaultSpace interface{}) (*packData, error) {
	d := &packData{
		packedDefaultSpace: encodeValues2(KeySpaceNo, defaultSpace),
	}

	return d, nil
}

func testPackData() *packData {
	d, _ := newPackData(0)
	return d
}
