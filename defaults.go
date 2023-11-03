package tarantool

import "time"

const (
	DefaultIndex = "primary"
	DefaultLimit = 100

	DefaultConnectTimeout = time.Second
	DefaultQueryTimeout   = time.Second
)

var (
	DefaultReaderBufSize = 16 * 1024
	DefaultWriterBufSize = 4 * 1024

	DefaultMaxPoolPacketSize = 64 * 1024
)
