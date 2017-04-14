package tarantool

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Upsert struct {
	Space interface{}
	Tuple []interface{}
	Set   []Operator
}

var _ Query = (*Upsert)(nil)

func (s *Upsert) Pack(data *packData, w io.Writer) (byte, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(3) // Space, Tuple, Update operators

	// Space
	if err = data.writeSpace(s.Space, w, encoder); err != nil {
		return BadRequest, err
	}

	// Tuple (to insert)
	encoder.EncodeUint32(KeyTuple)
	encoder.EncodeArrayLen(len(s.Tuple))
	for _, key := range s.Tuple {
		if err = encoder.Encode(key); err != nil {
			return BadRequest, err
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
				return BadRequest, err
			}
		}
	}

	return UpsertRequest, nil
}

func (q *Upsert) Unpack(r io.Reader) error {
	decoder := msgpack.NewDecoder(r)
	_, err := decoder.DecodeInterface()
	return err
}
