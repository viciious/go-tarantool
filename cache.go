package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// cache precompiled
type packCache struct {
	vSelectDefaultSpace []byte
}

func (c *packCache) SelectDefaultSpace(space interface{}) []byte {
	if c.vSelectDefaultSpace != nil {
		return c.vSelectDefaultSpace
	}

	var bodyBuffer bytes.Buffer
	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeUint32(KeySpaceNo)
	encoder.Encode(space)

	c.vSelectDefaultSpace = bodyBuffer.Bytes()
	return c.vSelectDefaultSpace
}
