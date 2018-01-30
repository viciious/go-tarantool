package tarantool

import "time"

const (
	DefaultIndex = "primary"
	DefaultLimit = 100

	DefaultConnectTimeout = time.Second
	DefaultQueryTimeout   = time.Second
)

var (
	DefaultReaderBufSize = 128 * 1024
	DefaultWriterBufSize = 4 * 1024
)
