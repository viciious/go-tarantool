package tarantool

import "github.com/tinylib/msgp/msgp"

// FetchSnapshot is the FETCH_SNAPSHOT command
type FetchSnapshot struct{}

var _ Query = (*FetchSnapshot)(nil)

func (q *FetchSnapshot) GetCommandID() uint {
	return FetchSnapshotCommand
}

// MarshalMsg implements msgp.Marshaler
func (q *FetchSnapshot) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	o = msgp.AppendMapHeader(o, 1)
	o = msgp.AppendUint(o, KeyVersionID)
	o = msgp.AppendUint(o, uint(version2_9_0))
	return o, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (q *FetchSnapshot) UnmarshalMsg([]byte) (buf []byte, err error) {
	return buf, ErrNotSupported
}
