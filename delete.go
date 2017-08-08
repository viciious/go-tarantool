package tarantool

import (
	"errors"
	"io"

	"github.com/vmihailenco/msgpack"
)

type Delete struct {
	Space    interface{}
	Index    interface{}
	Key      interface{}
	KeyTuple []interface{}
}

var _ Query = (*Delete)(nil)

func (q *Delete) Pack(data *packData, w io.Writer) (uint32, error) {
	var err error

	encoder := msgpack.NewEncoder(w)

	encoder.EncodeMapLen(3) // Space, Index, Key

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

	return DeleteRequest, nil
}

func (q *Delete) Unpack(r io.Reader) (err error) {
	var i int
	var k int
	var t uint

	q.Space = nil
	q.Index = 0
	q.Key = nil
	q.KeyTuple = nil

	decoder := msgpack.NewDecoder(r)

	if i, err = decoder.DecodeMapLen(); err != nil {
		return
	}

	for ; i > 0; i-- {
		if k, err = decoder.DecodeInt(); err != nil {
			return
		}

		switch k {
		case KeySpaceNo:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Space = int(t)
		case KeyIndexNo:
			if t, err = decoder.DecodeUint(); err != nil {
				return
			}
			q.Index = int(t)
		case KeyKey:
			if q.KeyTuple, err = decoder.DecodeSlice(); err != nil {
				return
			}
			if len(q.KeyTuple) == 1 {
				q.Key = q.KeyTuple[0]
				q.KeyTuple = nil
			}
		}
	}

	if q.Space == nil {
		return errors.New("Delete.Unpack: no space specified")
	}
	if q.Key == nil && q.KeyTuple == nil {
		return errors.New("Delete.Unpack: no tuple specified")
	}

	return nil
}
