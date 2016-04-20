package tarantool

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (s *Ping) Pack(requestID uint32, data *packData) ([]byte, error) {
	return packIproto(PingRequest, requestID, []byte{}), nil
}
