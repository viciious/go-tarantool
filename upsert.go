package tarantool

import (
	"io"

	"github.com/vmihailenco/msgpack"
)

type Upsert struct {
	Space interface{}
	Tuple []interface{}
	Set   []Operator
}

var _ Query = (*Upsert)(nil)

func (q *Upsert) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(3) // Space, Tuple, Update operators

	// Space
	if err = data.writeSpace(q.Space, w, encoder); err != nil {
		return ErrorFlag, err
	}

	// Tuple (to insert)
	encoder.EncodeUint(KeyTuple)
	encoder.EncodeArrayLen(len(q.Tuple))
	for _, key := range q.Tuple {
		if err = encoder.Encode(key); err != nil {
			return ErrorFlag, err
		}
	}

	// Update ops
	encoder.EncodeUint(KeyDefTuple)
	encoder.EncodeArrayLen(len(q.Set))
	for _, op := range q.Set {
		t := op.AsTuple()
		encoder.EncodeArrayLen(len(t))
		for _, value := range t {
			if err = encoder.Encode(value); err != nil {
				return ErrorFlag, err
			}
		}
	}

	return UpsertRequest, nil
}

func (q *Upsert) Unpack(r io.Reader) error {
	decoder := msgpack.NewDecoder(r)
	decoder.UseDecodeInterfaceLoose(true)

	_, err := decoder.DecodeInterface()
	return err
}
