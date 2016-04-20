package tarantool

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Delete struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
}

var _ Query = (*Delete)(nil)

func (s *Delete) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(3) // Space, Index, Key

	// Space
	if err = data.writeSpace(s.Space, bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Index
	if err = data.writeIndex(s.Space, s.Index, bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Key
	if s.Key != nil {
		bodyBuffer.Write(data.packedSingleKey)
		if err = encoder.Encode(s.Key); err != nil {
			return nil, err
		}
	} else if s.KeyTuple != nil {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(len(s.KeyTuple))
		for _, key := range s.KeyTuple {
			if err = encoder.Encode(key); err != nil {
				return nil, err
			}
		}
	}

	return packIproto(DeleteRequest, requestID, bodyBuffer.Bytes()), nil
}
