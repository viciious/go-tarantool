package tarantool

import "sync"

const requestMapShardNum = 16

type requestMapShard struct {
	sync.Mutex
	data map[uint32]*request
}
type requestMap struct {
	shard []*requestMapShard
}

func newRequestMap() *requestMap {
	shard := make([]*requestMapShard, requestMapShardNum)

	for i := 0; i < requestMapShardNum; i++ {
		shard[i] = &requestMapShard{
			data: make(map[uint32]*request),
		}
	}

	return &requestMap{
		shard: shard,
	}

}

// Put returns old request associated with given key
func (m *requestMap) Put(key uint32, value *request) *request {
	shard := m.shard[key%requestMapShardNum]
	shard.Lock()
	oldValue := shard.data[key]
	shard.data[key] = value
	shard.Unlock()
	return oldValue
}

// Pop returns request associated with given key and remove it from map
func (m *requestMap) Pop(key uint32) *request {
	shard := m.shard[key%requestMapShardNum]
	shard.Lock()
	value, exists := shard.data[key]
	if exists {
		delete(shard.data, key)
	}
	shard.Unlock()
	return value
}

func (m *requestMap) CleanUp(clearCallback func(*request)) {
	for i := 0; i < requestMapShardNum; i++ {
		shard := m.shard[i]
		shard.Lock()

		for requestID, req := range shard.data {
			delete(shard.data, requestID)
			clearCallback(req)
		}

		shard.Unlock()
	}
}
