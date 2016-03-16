package tnt

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Update struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
	Update   []Operator
}

var _ Query = (*Update)(nil)

func (s *Update) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(4) // Space, Index, Key, Update operators

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

	// Update
	encoder.EncodeUint32(KeyTuple)
	encoder.EncodeArrayLen(len(s.Update))
	for _, op := range s.Update {
		t := op.AsTuple()
		encoder.EncodeArrayLen(len(t))
		for _, value := range t {
			if err = encoder.Encode(value); err != nil {
				return nil, err
			}
		}
	}

	return packIproto(UpdateRequest, requestID, bodyBuffer.Bytes()), nil
}
