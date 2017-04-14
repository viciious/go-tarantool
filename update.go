package tarantool

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Update struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
	Set      []Operator
}

var _ Query = (*Update)(nil)

func (s *Update) Pack(data *packData, w io.Writer) (byte, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(4) // Space, Index, Key, Update operators

	// Space
	if err = data.writeSpace(s.Space, w, encoder); err != nil {
		return BadRequest, err
	}

	// Index
	if err = data.writeIndex(s.Space, s.Index, w, encoder); err != nil {
		return BadRequest, err
	}

	// Key
	if s.Key != nil {
		w.Write(data.packedSingleKey)
		if err = encoder.Encode(s.Key); err != nil {
			return BadRequest, err
		}
	} else if s.KeyTuple != nil {
		encoder.EncodeUint32(KeyKey)
		encoder.EncodeArrayLen(len(s.KeyTuple))
		for _, key := range s.KeyTuple {
			if err = encoder.Encode(key); err != nil {
				return BadRequest, err
			}
		}
	}

	// Update
	encoder.EncodeUint32(KeyTuple)
	encoder.EncodeArrayLen(len(s.Set))
	for _, op := range s.Set {
		t := op.AsTuple()
		encoder.EncodeArrayLen(len(t))
		for _, value := range t {
			if err = encoder.Encode(value); err != nil {
				return BadRequest, err
			}
		}
	}

	return UpdateRequest, nil
}

func (q *Update) Unpack(r io.Reader) error {
	decoder := msgpack.NewDecoder(r)
	_, err := decoder.DecodeInterface()
	return err
}
