package tarantool

import "io"

var packetPool *packedPacketPool

func init() {
	packetPool = newPackedPacketPool()
}

type Query interface {
	Pack(data *packData, w io.Writer) (uint32, error)
	Unpack(r io.Reader) error
}

type request struct {
	query     Query
	replyChan chan *Result
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
type VectorClock []int64

// NewVectorClockFrom returns VectorClock with clocks equal to the given lsn elements sequentially.
// Empty VectorClock would be returned if no lsn elements is given.
func NewVectorClock(lsns ...int64) VectorClock {
	if len(lsns) == 0 {
		return make([]int64, 0, VClockMax)
	}
	// zero index is reserved
	vc := make([]int64, len(lsns)+1, VClockMax)
	copy(vc[1:], lsns)
	return vc
}

// Follow the clocks.
// Update vector clock with given clock part.
func (vc *VectorClock) Follow(id uint32, lsn int64) bool {
	if id >= uint32(cap(*vc)) || lsn < 0 {
		return false
	}
	// extend vector by elements needed
	if id >= uint32(len(*vc)) {
		*vc = (*vc)[:id+1]
	}
	(*vc)[id] = lsn
	return true
}

// LSN is the sum of the Clocks.
func (vc VectorClock) LSN() int64 {
	result := int64(0)
	for _, lsn := range vc {
		result += lsn
	}
	return result
}

// Has VectorClock specified ID?
func (vc VectorClock) Has(id uint32) bool {
	return id < uint32(len(vc))
}
