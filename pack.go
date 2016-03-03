package tnt

var packedInt0 = Uint32(0)
var packedInt1 = Uint32(1)

func packLittle(value uint, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := 0; i < bytes; i++ {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

func packBig(value int, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := bytes - 1; i >= 0; i-- {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

// Uint32 is alias for PackL
func Uint32(value uint32) []byte {
	return packLittle(uint(value), 4)
}
