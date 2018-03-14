package tarantool

func packIproto(code int, requestID uint32) *binaryPacket {
	pp := packetPool.Get()
	pp.requestID = requestID
	pp.code = uint32(code)
	return pp
}

func packIprotoError(errCode int, requestID uint32) *binaryPacket {
	return packIproto(ErrorFlag|errCode, requestID)
}

func packIprotoOk(requestID uint32) *binaryPacket {
	pp := packIproto(OKRequest, requestID)
	copy(pp.body, emptyBody)
	return pp
}
