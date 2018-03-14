package tarantool

import "io"

type Ping struct {
}

var _ Query = (*Ping)(nil)

func (q *Ping) Pack(data *packData, w io.Writer) (uint32, error) {
	return PingRequest, nil
}

func (q Ping) PackMsg(data *packData, b []byte) (o []byte, code uint32, err error) {
	return b, PingRequest, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (q *Ping) UnmarshalBinary(data []byte) (err error) {
	_, err = q.UnmarshalMsg(data)
	return err
}

// UnmarshalMsg implements msgp.Unmarshaller
func (q *Ping) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, nil
}
