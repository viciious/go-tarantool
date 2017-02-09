package tarantool

import "bytes"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (s *Ping) Pack(data *packData) (byte, []byte, error) {
	return PingRequest, []byte{}, nil
}

func (q *Ping) Unpack(r *bytes.Buffer) error {
	return nil
}
