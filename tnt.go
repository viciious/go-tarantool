package tarantool

import (
	"expvar"
	"sync/atomic"
	"time"
)

var packetPool *BinaryPacketPool
var requestPool *cappedRequestPool
var defaultPackData *packData

func init() {
	packetPool = newBinaryPacketPool()
	requestPool = newCappedRequestPool()
	defaultPackData = newPackData(10000)
}

type request struct {
	opaque    interface{}
	replyChan chan *AsyncResult
	packet    *BinaryPacket
	startedAt time.Time
}

type QueryCompleteFn func(interface{}, time.Duration)

type AsyncResult struct {
	ErrorCode    uint
	Error        error
	BinaryPacket *BinaryPacket
	Connection   *Connection
	Opaque       interface{}
}

type PerfCount struct {
	NetRead       *expvar.Int
	NetWrite      *expvar.Int
	NetPacketsIn  *expvar.Int
	NetPacketsOut *expvar.Int
	QueryTimeouts *expvar.Int
	QueryComplete QueryCompleteFn
}

// ReplicaSet is used to store params of the Replica Set.
type ReplicaSet struct {
	UUID      string
	Instances []string // Instances is read-only set of the instances uuid
}

// NewReplicaSet returns empty ReplicaSet.
func NewReplicaSet() ReplicaSet {
	return ReplicaSet{Instances: make([]string, 0, ReplicaSetMaxSize)}
}

// SetInstance uuid in instance set.
func (rs *ReplicaSet) SetInstance(id uint32, uuid string) bool {
	if id >= uint32(cap(rs.Instances)) || len(uuid) != UUIDStrLength {
		return false
	}
	// extend vector by elements needed
	if id >= uint32(len(rs.Instances)) {
		rs.Instances = rs.Instances[:id+1]
	}
	rs.Instances[id] = uuid
	return true
}

// Has ReplicaSet specified instance?
func (rs *ReplicaSet) Has(id uint32) bool {
	return id < uint32(len(rs.Instances))
}

// VectorClock is used to store logical clocks (direct dependency clock implementation).
// Zero index is always reserved for internal use.
// You can get any lsn indexing VectorClock by instance ID directly (without any index offset).
// One can count instances in vector just using built-in len function.
type VectorClock []uint64

// NewVectorClock returns VectorClock with clocks equal to the given lsn elements sequentially.
// Empty VectorClock would be returned if no lsn elements is given.
func NewVectorClock(lsns ...uint64) VectorClock {
	if len(lsns) == 0 {
		return make([]uint64, 0, VClockMax)
	}
	// zero index is reserved
	vc := make([]uint64, len(lsns)+1, VClockMax)
	copy(vc[1:], lsns)
	return vc
}

// Follow the clocks.
// Update vector clock with given clock part.
func (vc *VectorClock) Follow(id uint32, lsn uint64) bool {
	if id >= uint32(cap(*vc)) {
		return false
	}
	// extend vector by elements needed
	if id >= uint32(len(*vc)) {
		*vc = (*vc)[:id+1]
	}
	atomic.StoreUint64(&(*vc)[id], lsn)
	return true
}

func (vc VectorClock) Clone() VectorClock {
	clone := make([]uint64, len(vc), cap(vc))
	for i := 0; i < len(vc); i++ {
		clone[i] = atomic.LoadUint64(&vc[i])
	}
	return clone
}

// LSN is the sum of the Clocks.
func (vc VectorClock) LSN() uint64 {
	result := uint64(0)
	for _, lsn := range vc {
		result += lsn
	}
	return result
}

// Has VectorClock specified ID?
func (vc VectorClock) Has(id uint32) bool {
	return id < uint32(len(vc))
}
