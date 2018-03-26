package tarantool

import (
	"io"
	"sync/atomic"
)

type CountedReader struct {
	r io.Reader
	c *uint64
}

func NewCountedReader(r io.Reader, c *uint64) *CountedReader {
	return &CountedReader{r, c}
}

func (cr *CountedReader) Read(p []byte) (int, error) {
	atomic.AddUint64(cr.c, 1)
	return cr.r.Read(p)
}

type CountedWriter struct {
	w io.Writer
	c *uint64
}

func NewCountedWriter(w io.Writer, c *uint64) *CountedWriter {
	return &CountedWriter{w, c}
}

func (cw *CountedWriter) Write(p []byte) (int, error) {
	atomic.AddUint64(cw.c, 1)
	return cw.w.Write(p)
}
