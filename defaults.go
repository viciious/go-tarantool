package tarantool

import "time"

const (
	DefaultIndex = "primary"
)

var (
	DefaultLimit = 250

	DefaultConnectTimeout = time.Second
	DefaultQueryTimeout   = time.Second

	DefaultReaderBufSize = 16 * 1024
	DefaultWriterBufSize = 4 * 1024

	DefaultMaxPoolPacketSize = 64 * 1024
)
