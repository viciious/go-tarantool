package tarantool

import "bytes"

type bufferPoolRecord struct {
	buffer *bytes.Buffer
	pool   chan *bufferPoolRecord
}

type bufferPool struct {
	queue chan *bufferPoolRecord
}

func NewBufferPool() *bufferPool {
	return &bufferPool{
		queue: make(chan *bufferPoolRecord, 10240),
	}
}

func (r *bufferPoolRecord) Release() {
	r.buffer.Reset()
	select {
	case r.pool <- r:
		return
	default:
		r.buffer = nil
		r.pool = nil
	}
}

func (p *bufferPool) Get(size int) *bufferPoolRecord {
	select {
	case r := <-p.queue:
		r.buffer.Grow(size)
		r.buffer.Reset()
		return r
	default:
		b := make([]byte, size)[:0]
		return &bufferPoolRecord{
			buffer: bytes.NewBuffer(b),
			pool:   p.queue,
		}
	}
}

func (p *bufferPool) Close() {
	close(p.queue)
}
