package tarantool

import "io"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (s *Ping) Pack(data *packData, w io.Writer) (byte, error) {
	return PingRequest, nil
}

func (q *Ping) Unpack(r io.Reader) error {
	return nil
}
