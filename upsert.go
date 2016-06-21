package tarantool

import (
	"bytes"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Upsert struct {
	Space interface{}
	Tuple []interface{}
	Set   []Operator
}

var _ Query = (*Upsert)(nil)

func (s *Upsert) Pack(requestID uint32, data *packData) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var err error

	encoder := msgpack.NewEncoder(&bodyBuffer)

	encoder.EncodeMapLen(3) // Space, Tuple, Update operators

	// Space
	if err = data.writeSpace(s.Space, &bodyBuffer, encoder); err != nil {
		return nil, err
	}

	// Tuple (to insert)
	encoder.EncodeUint32(KeyTuple)
	encoder.EncodeArrayLen(len(s.Tuple))
	for _, key := range s.Tuple {
		if err = encoder.Encode(key); err != nil {
			return nil, err
		}
	}

	// Update ops
	encoder.EncodeUint32(KeyDefTuple)
	encoder.EncodeArrayLen(len(s.Set))
	for _, op := range s.Set {
		t := op.AsTuple()
		encoder.EncodeArrayLen(len(t))
		for _, value := range t {
			if err = encoder.Encode(value); err != nil {
				return nil, err
			}
		}
	}

	return packIproto(UpsertRequest, requestID, bodyBuffer.Bytes()), nil
}
