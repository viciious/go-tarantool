package tarantool

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodePacket(t *testing.T) {
	assert := assert.New(t)

	body := []byte("\x83\x00\xce\x00\x00\x00\x00\x01\xcf\x00\x00\x00\x00\x00\x00\x00\x03\x05\xce\x00\x00\x006\x810\xdd\x00\x00\x00\x03\x92\x01\xacFirst record\x92\x02\xa5Music\x93\x03\xa6Length]")

	pp := &binaryPacket{body: body}
	res := &pp.packet

	err := res.UnmarshalBinary(pp.body)
	assert.NoError(err)
	assert.EqualValues(3, res.requestID)
	assert.EqualValues(0, res.Result.ErrorCode)
	//assert.EqualValues([][]interface{}{[]interface{}{int64(1), "First record"}, []interface{}{int64(2), "Music"}, []interface{}{int64(3), "Length", int64(93)}}, res.Result.Data)
}

func BenchmarkDecodePacket(b *testing.B) {
	b.ReportAllocs()
	body := []byte("\x83\x00\xce\x00\x00\x00\x00\x01\xcf\x00\x00\x00\x00\x00\x00\x00\x03\x05\xce\x00\x00\x006\x810\xdd\x00\x00\x00\x03\x92\x01\xacFirst record\x92\x02\xa5Music\x93\x03\xa6Length]")
	pp := &binaryPacket{body: body}
	res := &pp.packet

	for i := 0; i < b.N; i++ {
		err := res.UnmarshalBinary(pp.body)
		if err != nil || res.requestID != 3 {
			b.FailNow()
		}
	}
}

func BenchmarkDecodeHeader(b *testing.B) {
	b.ReportAllocs()
	body := []byte("\x83\x00\xce\x00\x00\x00\x00\x01\xcf\x00\x00\x00\x00\x00\x00\x00\x03\x05\xce\x00\x00\x006\x810\xdd\x00\x00\x00\x03\x92\x01\xacFirst record\x92\x02\xa5Music\x93\x03\xa6Length]")
	pp := &binaryPacket{body: body}
	pack := &pp.packet

	for i := 0; i < b.N; i++ {
		_, err := pack.UnmarshalBinaryHeader(pp.body)
		if err != nil || pack.requestID != 3 {
			b.FailNow()
		}
	}
}
