package tarantool

import (
	"bytes"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Call struct {
	Name  string
	Tuple []interface{}
}

var _ Query = (*Call)(nil)

func (s *Call) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(2) // Name, Tuple

	// Name
	encoder.EncodeUint32(KeyFunctionName)
	encoder.EncodeString(s.Name)

	if s.Tuple != nil {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(len(s.Tuple))
		for _, key := range s.Tuple {
			if err = encoder.Encode(key); err != nil {
				return nil, err
			}
		}
	} else {
		encoder.EncodeUint32(KeyTuple)
		encoder.EncodeArrayLen(0)
	}

	return packIproto(CallRequest, requestID, bodyBuffer.Bytes()), nil
}

func (q *Call) Unpack(decoder *msgpack.Decoder) error {
	return nil
}
