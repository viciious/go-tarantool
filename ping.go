package tarantool

import "gopkg.in/vmihailenco/msgpack.v2"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (s *Ping) Pack(requestID uint32, data *packData) ([]byte, error) {
	return packIproto(PingRequest, requestID, []byte{}), nil
}

func (q *Ping) Unpack(decoder *msgpack.Decoder) error {
	return nil
}
