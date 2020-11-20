package tarantool

import (
	"expvar"
	"io"
)

type CountedReader struct {
	r io.Reader
	c *expvar.Int
}

func NewCountedReader(r io.Reader, c *expvar.Int) *CountedReader {
	return &CountedReader{r, c}
}

func (cr *CountedReader) Read(p []byte) (int, error) {
	cr.c.Add(1)
	return cr.r.Read(p)
}

type CountedWriter struct {
	w io.Writer
	c *expvar.Int
}

func NewCountedWriter(w io.Writer, c *expvar.Int) *CountedWriter {
	return &CountedWriter{w, c}
}

func (cw *CountedWriter) Write(p []byte) (int, error) {
	cw.c.Add(1)
	return cw.w.Write(p)
}
