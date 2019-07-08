package tarantool

import "io"

// VoteRequest which we send to master before Join in Tarantool >= 1.9.0
type Vote struct{}

var _ Query = (*Vote)(nil)

// Pack implements a part of the Query interface
func (q *Vote) Pack(data *packData, w io.Writer) (uint32, error) {
	return VoteRequest, nil
}

// Unpack implements a part of the Query interface
func (q *Vote) Unpack(r io.Reader) (err error) {
	return ErrNotSupported
}
