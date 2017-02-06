package tarantool

import "bytes"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (s *Ping) Pack(requestID uint32, data *packData) ([]byte, error) {
	return packIproto(PingRequest, requestID, []byte{}), nil
}

func (q *Ping) Unpack(r *bytes.Buffer) error {
	return nil
}
