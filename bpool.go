package tarantool

import "bytes"

type Pool struct {
	queue      chan *PoolRecord
	recordSize int
}

type PoolRecord struct {
	Body   []byte
	Buffer *bytes.Buffer
	pool   chan *PoolRecord
}

func NewPool(recordSize int, recordCount int) *Pool {
	return &Pool{
		recordSize: recordSize,
		queue:      make(chan *PoolRecord, recordCount),
	}
}

// Release возвращает запись обратно в пул. После вызова Release использовать запись дальше не стоит
func (r *PoolRecord) Release() {
	r.Buffer = nil

	select {
	case r.pool <- r:
		return
	// места в пуле не оказалось. Оставляем объект GC
	default:
		r.pool = nil
	}
}

// Get возвращает Record из пула. Или создает новый инстанс
func (p *Pool) Get() *PoolRecord {
	select {
	case r := <-p.queue:
		return r
	default:
		return &PoolRecord{
			Body: make([]byte, p.recordSize),
			pool: p.queue,
		}

	}
}

// Close закрывает канал-очередь объектов, считывает из него все (просто на всякий случай)
func (p *Pool) Close() {
	close(p.queue)

	for {
		_, opened := <-p.queue
		if !opened {
			return
		}
	}
}
