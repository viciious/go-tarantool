package tarantool

import "io"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (q *Ping) Pack(data *packData, w io.Writer) (uint32, error) {
	return PingRequest, nil
}

func (q *Ping) Unpack(r io.Reader) error {
	return nil
}
