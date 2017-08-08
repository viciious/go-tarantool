package tarantool

import (
	"io"

	"github.com/vmihailenco/msgpack"
)

type Update struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
	Set      []Operator
}

var _ Query = (*Update)(nil)

func (q *Update) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(4) // Space, Index, Key, Update operators

	// Space
	if err = data.writeSpace(q.Space, w, encoder); err != nil {
		return ErrorFlag, err
	}

	// Index
	if err = data.writeIndex(q.Space, q.Index, w, encoder); err != nil {
		return ErrorFlag, err
	}

	// Key
	if q.Key != nil {
		w.Write(data.packedSingleKey)
		if err = encoder.Encode(q.Key); err != nil {
			return ErrorFlag, err
		}
	} else if q.KeyTuple != nil {
		encoder.EncodeUint(KeyKey)
		encoder.EncodeArrayLen(len(q.KeyTuple))
		for _, key := range q.KeyTuple {
			if err = encoder.Encode(key); err != nil {
				return ErrorFlag, err
			}
		}
	}

	// Update
	encoder.EncodeUint(KeyTuple)
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

	return UpdateRequest, nil
}

func (q *Update) Unpack(r io.Reader) error {
	decoder := msgpack.NewDecoder(r)
	_, err := decoder.DecodeInterface()
	return err
}
