package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Replace struct {
	Space interface{}
	Tuple []interface{}
}

var _ Query = (*Replace)(nil)

func (s *Replace) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(2) // Space, Tuple

	// Space
	if err = data.writeSpace(s.Space, bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Tuple
	if s.Tuple != nil {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(len(s.Tuple))
		for _, value := range s.Tuple {
			if err = encoder.Encode(value); err != nil {
				return nil, err
			}
		}
	}

	return packIproto(ReplaceRequest, requestID, bodyBuffer.Bytes()), nil
}
