package tarantool

/*
Forked from https://github.com/tarantool/go-tarantool/blob/master/const.go
*/

const (
	OKCommand            = uint(0)
	SelectCommand        = uint(1)
	InsertCommand        = uint(2)
	ReplaceCommand       = uint(3)
	UpdateCommand        = uint(4)
	DeleteCommand        = uint(5)
	CallCommand          = uint(6)
	AuthCommand          = uint(7)
	EvalCommand          = uint(8)
	UpsertCommand        = uint(9)
	Call17Command        = uint(10) // Tarantool >= 1.7.2
	PingCommand          = uint(64)
	JoinCommand          = uint(65)
	SubscribeCommand     = uint(66)
	VoteCommand          = uint(68) // Tarantool >= 1.9.0
	FetchSnapshotCommand = uint(69) // for starting anonymous replication. Tarantool >= 2.3.1
	RegisterCommand      = uint(70) // for leaving anonymous replication (anon => normal replica). Tarantool >= 2.3.1
	ErrorFlag            = uint(0x8000)
)

const (
	KeyCode           = uint(0x00)
	KeySync           = uint(0x01)
	KeyInstanceID     = uint(0x02)
	KeyLSN            = uint(0x03)
	KeyTimestamp      = uint(0x04)
	KeySchemaID       = uint(0x05)
	KeySpaceNo        = uint(0x10)
	KeyIndexNo        = uint(0x11)
	KeyLimit          = uint(0x12)
	KeyOffset         = uint(0x13)
	KeyIterator       = uint(0x14)
	KeyKey            = uint(0x20)
	KeyTuple          = uint(0x21)
	KeyFunctionName   = uint(0x22)
	KeyUserName       = uint(0x23)
	KeyInstanceUUID   = uint(0x24)
	KeyReplicaSetUUID = uint(0x25)
	KeyVClock         = uint(0x26)
	KeyExpression     = uint(0x27)
	KeyDefTuple       = uint(0x28)
	KeyBallot         = uint(0x29) // Tarantool >= 1.9.0
	KeyData           = uint(0x30)
	KeyError          = uint(0x31)
	KeyReplicaAnon    = uint(0x50) // Tarantool >= 2.3.1
)

const (
	// https://github.com/fl00r/go-tarantool-1.6/issues/2
	IterEq            = uint8(0) // key == x ASC order
	IterReq           = uint8(1) // key == x DESC order
	IterAll           = uint8(2) // all tuples
	IterLt            = uint8(3) // key < x
	IterLe            = uint8(4) // key <= x
	IterGe            = uint8(5) // key >= x
	IterGt            = uint8(6) // key > x
	IterBitsAllSet    = uint8(7) // all bits from x are set in key
	IterBitsAnySet    = uint8(8) // at least one x's bit is set
	IterBitsAllNotSet = uint8(9) // all bits are not set
)

const (
	SchemaKeyClusterUUID = "cluster"
	ReplicaSetMaxSize    = 32
	VClockMax            = ReplicaSetMaxSize
	UUIDStrLength        = 36
)

const (
	SpaceSchema    = uint(272)
	SpaceSpace     = uint(280)
	ViewSpace      = uint(281)
	SpaceIndex     = uint(288)
	ViewIndex      = uint(289)
	SpaceFunc      = uint(296)
	SpaceUser      = uint(304)
	SpacePriv      = uint(312)
	SpaceCluster   = uint(320)
	SpaceSystemMax = uint(511)
)

// Tarantool server error codes
const (
	ErrUnknown                       = uint(0x00) // Unknown error
	ErrIllegalParams                 = uint(0x01) // Illegal parameters, %s
	ErrMemoryIssue                   = uint(0x02) // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = uint(0x03) // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = uint(0x04) // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = uint(0x05) // %s does not support %s
	ErrNonmaster                     = uint(0x06) // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = uint(0x07) // Can't modify data because this server is in read-only mode.
	ErrInjection                     = uint(0x08) // Error injection '%s'
	ErrCreateSpace                   = uint(0x09) // Failed to create space '%s': %s
	ErrSpaceExists                   = uint(0x0a) // Space '%s' already exists
	ErrDropSpace                     = uint(0x0b) // Can't drop space '%s': %s
	ErrAlterSpace                    = uint(0x0c) // Can't modify space '%s': %s
	ErrIndexType                     = uint(0x0d) // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = uint(0x0e) // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = uint(0x0f) // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = uint(0x10) // Tuple format limit reached: %u
	ErrDropPrimaryKey                = uint(0x11) // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = uint(0x12) // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = uint(0x13) // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = uint(0x14) // Invalid MsgPack - %s
	ErrProcRet                       = uint(0x15) // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = uint(0x16) // Tuple/Key must be MsgPack array
	ErrFieldType                     = uint(0x17) // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = uint(0x18) // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = uint(0x19) // SPLICE error on field %u: %s
	ErrArgType                       = uint(0x1a) // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = uint(0x1b) // Tuple is too long %u
	ErrUnknownUpdateOp               = uint(0x1c) // Unknown UPDATE operation
	ErrUpdateField                   = uint(0x1d) // Field %u UPDATE error: %s
	ErrFiberStack                    = uint(0x1e) // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = uint(0x1f) // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = uint(0x20) // %s
	ErrNoSuchProc                    = uint(0x21) // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = uint(0x22) // Trigger is not found
	ErrNoSuchIndex                   = uint(0x23) // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = uint(0x24) // Space '%s' does not exist
	ErrNoSuchField                   = uint(0x25) // Field %d was not found in the tuple
	ErrSpaceFieldCount               = uint(0x26) // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = uint(0x27) // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = uint(0x28) // Failed to write to disk
	ErrMoreThanOneTuple              = uint(0x29) // More than one tuple found by get()
	ErrAccessDenied                  = uint(0x2a) // %s access denied for user '%s'
	ErrCreateUser                    = uint(0x2b) // Failed to create user '%s': %s
	ErrDropUser                      = uint(0x2c) // Failed to drop user '%s': %s
	ErrNoSuchUser                    = uint(0x2d) // User '%s' is not found
	ErrUserExists                    = uint(0x2e) // User '%s' already exists
	ErrPasswordMismatch              = uint(0x2f) // Incorrect password supplied for user '%s'
	ErrUnknownRequestType            = uint(0x30) // Unknown request type %u
	ErrUnknownSchemaObject           = uint(0x31) // Unknown object type '%s'
	ErrCreateFunction                = uint(0x32) // Failed to create function '%s': %s
	ErrNoSuchFunction                = uint(0x33) // Function '%s' does not exist
	ErrFunctionExists                = uint(0x34) // Function '%s' already exists
	ErrFunctionAccessDenied          = uint(0x35) // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = uint(0x36) // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = uint(0x37) // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = uint(0x38) // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = uint(0x39) // Space engine '%s' does not exist
	ErrReloadCfg                     = uint(0x3a) // Can't set option '%s' dynamically
	ErrCfg                           = uint(0x3b) // Incorrect value for option '%s': %s
	ErrSophia                        = uint(0x3c) // %s
	ErrLocalServerIsNotActive        = uint(0x3d) // Local server is not active
	ErrUnknownServer                 = uint(0x3e) // Server %s is not registered with the cluster
	ErrClusterIDMismatch             = uint(0x3f) // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUUID                   = uint(0x40) // Invalid UUID: %s
	ErrClusterIDIsRo                 = uint(0x41) // Can't reset cluster id: it is already assigned
	ErrReserved66                    = uint(0x42) // Reserved66
	ErrServerIDIsReserved            = uint(0x43) // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = uint(0x44) // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = uint(0x45) // Missing mandatory field '%s' in request
	ErrIdentifier                    = uint(0x46) // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = uint(0x47) // Can't drop function %u: %s
	ErrIteratorType                  = uint(0x48) // Unknown iterator type '%s'
	ErrReplicaMax                    = uint(0x49) // Replica count limit reached: %u
	ErrInvalidXlog                   = uint(0x4a) // Failed to read xlog: %lld
	ErrInvalidXlogName               = uint(0x4b) // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = uint(0x4c) // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = uint(0x4d) // Connection is not established
	ErrTimeout                       = uint(0x4e) // Timeout exceeded
	ErrActiveTransaction             = uint(0x4f) // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = uint(0x50) // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = uint(0x51) // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = uint(0x52) // Role '%s' is not found
	ErrRoleExists                    = uint(0x53) // Role '%s' already exists
	ErrCreateRole                    = uint(0x54) // Failed to create role '%s': %s
	ErrIndexExists                   = uint(0x55) // Index '%s' already exists
	ErrTupleRefOverflow              = uint(0x56) // Tuple reference counter overflow
	ErrRoleLoop                      = uint(0x57) // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = uint(0x58) // Incorrect grant arguments: %s
	ErrPrivGranted                   = uint(0x59) // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = uint(0x5a) // User '%s' already has role '%s'
	ErrPrivNotGranted                = uint(0x5b) // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = uint(0x5c) // User '%s' does not have role '%s'
	ErrMissingSnapshot               = uint(0x5d) // Can't find snapshot
	ErrCantUpdatePrimaryKey          = uint(0x5e) // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = uint(0x5f) // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = uint(0x60) // Setting password for guest user has no effect
	ErrTransactionConflict           = uint(0x61) // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = uint(0x62) // Unsupported role privilege '%s'
	ErrLoadFunction                  = uint(0x63) // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = uint(0x64) // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = uint(0x65) // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = uint(0x66) // ???
	ErrUnknownRtreeIndexDistanceType = uint(0x67) //Unknown RTREE index distance type %s
	ErrProtocol                      = uint(0x68) // %s
	ErrUpsertUniqueSecondaryKey      = uint(0x69) // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = uint(0x6a) // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = uint(0x6b) // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = uint(0x6c) // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = uint(0x6d) // Wrong schema version, current: %d, in request: %u
	ErrSlabAllocMax                  = uint(0x6e) // Failed to allocate %u bytes for tuple in the slab allocator: tuple is too large. Check 'slab_alloc_maximal' configuration option.
	ErrXLogGap                       = uint(0xdb) // Missing .xlog file between LSN %lld %s and %lld %s
)

const (
	GreetingSize = 128
)

const (
	ServerIdent = "Tarantool 1.6.8 (Binary)"
)

// Consts for Tarantool features which require version check
const (
	version1_7_0 = uint32(67328) // VersionID(1, 7, 0)
	version1_7_7 = uint32(67335) // VersionID(1, 7, 7)
	// 2.3.1 is min version for anonymous replication
	version2_3_1 = uint32(131841) // VersionID(2, 3, 1)
	// Add box.info.replication_anon
	version2_5_1 = uint32(132353) // VersionID(2, 5, 1)
	version2_8_0 = uint32(133120) // VersionID(2, 8, 0)
	version2_9_0 = uint32(133376) // VersionID(2, 9, 0)
)
