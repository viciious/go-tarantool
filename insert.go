package tarantool

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Insert struct {
	Space interface{}
	Tuple []interface{}
}

var _ Query = (*Insert)(nil)

func (s *Insert) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(2) // Space, Tuple

	// Space
	if err = data.writeSpace(s.Space, &bodyBuffer, encoder); err != nil {
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

	return packIproto(InsertRequest, requestID, bodyBuffer.Bytes()), nil
}

func (q *Insert) Unpack(decoder *msgpack.Decoder) error {
	return nil
}
