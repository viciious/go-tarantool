package tarantool

/*
Forked from https://github.com/tarantool/go-tarantool/blob/master/const.go
*/

const (
	OKRequest        = 0
	SelectRequest    = 1
	InsertRequest    = 2
	ReplaceRequest   = 3
	UpdateRequest    = 4
	DeleteRequest    = 5
	CallRequest      = 6
	AuthRequest      = 7
	EvalRequest      = 8
	UpsertRequest    = 9
	PingRequest      = 64
	JoinCommand      = 65
	SubscribeRequest = 66
	ErrorFlag        = 0x8000
)

const (
	KeyCode           = 0x00
	KeySync           = 0x01
	KeyInstanceID     = 0x02
	KeyLSN            = 0x03
	KeyTimestamp      = 0x04
	KeySchemaID       = 0x05
	KeySpaceNo        = 0x10
	KeyIndexNo        = 0x11
	KeyLimit          = 0x12
	KeyOffset         = 0x13
	KeyIterator       = 0x14
	KeyKey            = 0x20
	KeyTuple          = 0x21
	KeyFunctionName   = 0x22
	KeyUserName       = 0x23
	KeyInstanceUUID   = 0x24
	KeyReplicaSetUUID = 0x25
	KeyVClock         = 0x26
	KeyExpression     = 0x27
	KeyDefTuple       = 0x28
	KeyData           = 0x30
	KeyError          = 0x31
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
	OkCode            = 0
	PacketLengthBytes = 5

	SchemaKeyClusterUUID = "cluster"
	ReplicaSetMaxSize    = 32
	VClockMax            = ReplicaSetMaxSize
	UUIDStrLength        = 36
)

const (
	SpaceSchema    = 272
	SpaceSpace     = 280
	ViewSpace      = 281
	SpaceIndex     = 288
	ViewIndex      = 289
	SpaceFunc      = 296
	SpaceUser      = 304
	SpacePriv      = 312
	SpaceCluster   = 320
	SpaceSystemMax = 511
)

// Tarantool server error codes
const (
	ErrUnknown                       = iota // Unknown error
	ErrIllegalParams                 = iota // Illegal parameters, %s
	ErrMemoryIssue                   = iota // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = iota // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = iota // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = iota // %s does not support %s
	ErrNonmaster                     = iota // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = iota // Can't modify data because this server is in read-only mode.
	ErrInjection                     = iota // Error injection '%s'
	ErrCreateSpace                   = iota // Failed to create space '%s': %s
	ErrSpaceExists                   = iota // Space '%s' already exists
	ErrDropSpace                     = iota // Can't drop space '%s': %s
	ErrAlterSpace                    = iota // Can't modify space '%s': %s
	ErrIndexType                     = iota // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = iota // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = iota // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = iota // Tuple format limit reached: %u
	ErrDropPrimaryKey                = iota // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = iota // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = iota // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = iota // Invalid MsgPack - %s
	ErrProcRet                       = iota // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = iota // Tuple/Key must be MsgPack array
	ErrFieldType                     = iota // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = iota // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = iota // SPLICE error on field %u: %s
	ErrArgType                       = iota // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = iota // Tuple is too long %u
	ErrUnknownUpdateOp               = iota // Unknown UPDATE operation
	ErrUpdateField                   = iota // Field %u UPDATE error: %s
	ErrFiberStack                    = iota // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = iota // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = iota // %s
	ErrNoSuchProc                    = iota // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = iota // Trigger is not found
	ErrNoSuchIndex                   = iota // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = iota // Space '%s' does not exist
	ErrNoSuchField                   = iota // Field %d was not found in the tuple
	ErrSpaceFieldCount               = iota // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = iota // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = iota // Failed to write to disk
	ErrMoreThanOneTuple              = iota // More than one tuple found by get()
	ErrAccessDenied                  = iota // %s access denied for user '%s'
	ErrCreateUser                    = iota // Failed to create user '%s': %s
	ErrDropUser                      = iota // Failed to drop user '%s': %s
	ErrNoSuchUser                    = iota // User '%s' is not found
	ErrUserExists                    = iota // User '%s' already exists
	ErrPasswordMismatch              = iota // Incorrect password supplied for user '%s'
	ErrUnknownRequestType            = iota // Unknown request type %u
	ErrUnknownSchemaObject           = iota // Unknown object type '%s'
	ErrCreateFunction                = iota // Failed to create function '%s': %s
	ErrNoSuchFunction                = iota // Function '%s' does not exist
	ErrFunctionExists                = iota // Function '%s' already exists
	ErrFunctionAccessDenied          = iota // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = iota // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = iota // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = iota // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = iota // Space engine '%s' does not exist
	ErrReloadCfg                     = iota // Can't set option '%s' dynamically
	ErrCfg                           = iota // Incorrect value for option '%s': %s
	ErrSophia                        = iota // %s
	ErrLocalServerIsNotActive        = iota // Local server is not active
	ErrUnknownServer                 = iota // Server %s is not registered with the cluster
	ErrClusterIDMismatch             = iota // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUUID                   = iota // Invalid UUID: %s
	ErrClusterIDIsRo                 = iota // Can't reset cluster id: it is already assigned
	ErrReserved66                    = iota // Reserved66
	ErrServerIDIsReserved            = iota // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = iota // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = iota // Missing mandatory field '%s' in request
	ErrIdentifier                    = iota // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = iota // Can't drop function %u: %s
	ErrIteratorType                  = iota // Unknown iterator type '%s'
	ErrReplicaMax                    = iota // Replica count limit reached: %u
	ErrInvalidXlog                   = iota // Failed to read xlog: %lld
	ErrInvalidXlogName               = iota // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = iota // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = iota // Connection is not established
	ErrTimeout                       = iota // Timeout exceeded
	ErrActiveTransaction             = iota // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = iota // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = iota // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = iota // Role '%s' is not found
	ErrRoleExists                    = iota // Role '%s' already exists
	ErrCreateRole                    = iota // Failed to create role '%s': %s
	ErrIndexExists                   = iota // Index '%s' already exists
	ErrTupleRefOverflow              = iota // Tuple reference counter overflow
	ErrRoleLoop                      = iota // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = iota // Incorrect grant arguments: %s
	ErrPrivGranted                   = iota // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = iota // User '%s' already has role '%s'
	ErrPrivNotGranted                = iota // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = iota // User '%s' does not have role '%s'
	ErrMissingSnapshot               = iota // Can't find snapshot
	ErrCantUpdatePrimaryKey          = iota // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = iota // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = iota // Setting password for guest user has no effect
	ErrTransactionConflict           = iota // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = iota // Unsupported role privilege '%s'
	ErrLoadFunction                  = iota // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = iota // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = iota // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = iota // ???
	ErrUnknownRtreeIndexDistanceType = iota //Unknown RTREE index distance type %s
	ErrProtocol                      = iota // %s
	ErrUpsertUniqueSecondaryKey      = iota // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = iota // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = iota // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = iota // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = iota // Wrong schema version, current: %d, in request: %u
	ErrSlabAllocMax                  = iota // Failed to allocate %u bytes for tuple in the slab allocator: tuple is too large. Check 'slab_alloc_maximal' configuration option.
)

const (
	GreetingSize = 128
)

const (
	ServerIdent = "Tarantool 1.6.8 (Binary)"
)
