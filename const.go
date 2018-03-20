package tarantool

/*
Forked from https://github.com/tarantool/go-tarantool/blob/master/const.go
*/

const (
	OKCommand        = 0
	SelectCommand    = 1
	InsertCommand    = 2
	ReplaceCommand   = 3
	UpdateCommand    = 4
	DeleteCommand    = 5
	CallCommand      = 6
	AuthCommand      = 7
	EvalCommand      = 8
	UpsertCommand    = 9
	PingCommand      = 64
	JoinCommand      = 65
	SubscribeCommand = 66
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
	ErrUnknown                       = 0x00 // Unknown error
	ErrIllegalParams                 = 0x01 // Illegal parameters, %s
	ErrMemoryIssue                   = 0x02 // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = 0x03 // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = 0x04 // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = 0x05 // %s does not support %s
	ErrNonmaster                     = 0x06 // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = 0x07 // Can't modify data because this server is in read-only mode.
	ErrInjection                     = 0x08 // Error injection '%s'
	ErrCreateSpace                   = 0x09 // Failed to create space '%s': %s
	ErrSpaceExists                   = 0x0a // Space '%s' already exists
	ErrDropSpace                     = 0x0b // Can't drop space '%s': %s
	ErrAlterSpace                    = 0x0c // Can't modify space '%s': %s
	ErrIndexType                     = 0x0d // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = 0x0e // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = 0x0f // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = 0x10 // Tuple format limit reached: %u
	ErrDropPrimaryKey                = 0x11 // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = 0x12 // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = 0x13 // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = 0x14 // Invalid MsgPack - %s
	ErrProcRet                       = 0x15 // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = 0x16 // Tuple/Key must be MsgPack array
	ErrFieldType                     = 0x17 // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = 0x18 // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = 0x19 // SPLICE error on field %u: %s
	ErrArgType                       = 0x1a // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = 0x1b // Tuple is too long %u
	ErrUnknownUpdateOp               = 0x1c // Unknown UPDATE operation
	ErrUpdateField                   = 0x1d // Field %u UPDATE error: %s
	ErrFiberStack                    = 0x1e // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = 0x1f // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = 0x20 // %s
	ErrNoSuchProc                    = 0x21 // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = 0x22 // Trigger is not found
	ErrNoSuchIndex                   = 0x23 // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = 0x24 // Space '%s' does not exist
	ErrNoSuchField                   = 0x25 // Field %d was not found in the tuple
	ErrSpaceFieldCount               = 0x26 // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = 0x27 // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = 0x28 // Failed to write to disk
	ErrMoreThanOneTuple              = 0x29 // More than one tuple found by get()
	ErrAccessDenied                  = 0x2a // %s access denied for user '%s'
	ErrCreateUser                    = 0x2b // Failed to create user '%s': %s
	ErrDropUser                      = 0x2c // Failed to drop user '%s': %s
	ErrNoSuchUser                    = 0x2d // User '%s' is not found
	ErrUserExists                    = 0x2e // User '%s' already exists
	ErrPasswordMismatch              = 0x2f // Incorrect password supplied for user '%s'
	ErrUnknownRequestType            = 0x30 // Unknown request type %u
	ErrUnknownSchemaObject           = 0x31 // Unknown object type '%s'
	ErrCreateFunction                = 0x32 // Failed to create function '%s': %s
	ErrNoSuchFunction                = 0x33 // Function '%s' does not exist
	ErrFunctionExists                = 0x34 // Function '%s' already exists
	ErrFunctionAccessDenied          = 0x35 // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = 0x36 // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = 0x37 // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = 0x38 // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = 0x39 // Space engine '%s' does not exist
	ErrReloadCfg                     = 0x3a // Can't set option '%s' dynamically
	ErrCfg                           = 0x3b // Incorrect value for option '%s': %s
	ErrSophia                        = 0x3c // %s
	ErrLocalServerIsNotActive        = 0x3d // Local server is not active
	ErrUnknownServer                 = 0x3e // Server %s is not registered with the cluster
	ErrClusterIDMismatch             = 0x3f // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUUID                   = 0x40 // Invalid UUID: %s
	ErrClusterIDIsRo                 = 0x41 // Can't reset cluster id: it is already assigned
	ErrReserved66                    = 0x42 // Reserved66
	ErrServerIDIsReserved            = 0x43 // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = 0x44 // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = 0x45 // Missing mandatory field '%s' in request
	ErrIdentifier                    = 0x46 // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = 0x47 // Can't drop function %u: %s
	ErrIteratorType                  = 0x48 // Unknown iterator type '%s'
	ErrReplicaMax                    = 0x49 // Replica count limit reached: %u
	ErrInvalidXlog                   = 0x4a // Failed to read xlog: %lld
	ErrInvalidXlogName               = 0x4b // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = 0x4c // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = 0x4d // Connection is not established
	ErrTimeout                       = 0x4e // Timeout exceeded
	ErrActiveTransaction             = 0x4f // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = 0x50 // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = 0x51 // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = 0x52 // Role '%s' is not found
	ErrRoleExists                    = 0x53 // Role '%s' already exists
	ErrCreateRole                    = 0x54 // Failed to create role '%s': %s
	ErrIndexExists                   = 0x55 // Index '%s' already exists
	ErrTupleRefOverflow              = 0x56 // Tuple reference counter overflow
	ErrRoleLoop                      = 0x57 // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = 0x58 // Incorrect grant arguments: %s
	ErrPrivGranted                   = 0x59 // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = 0x5a // User '%s' already has role '%s'
	ErrPrivNotGranted                = 0x5b // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = 0x5c // User '%s' does not have role '%s'
	ErrMissingSnapshot               = 0x5d // Can't find snapshot
	ErrCantUpdatePrimaryKey          = 0x5e // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = 0x5f // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = 0x60 // Setting password for guest user has no effect
	ErrTransactionConflict           = 0x61 // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = 0x62 // Unsupported role privilege '%s'
	ErrLoadFunction                  = 0x63 // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = 0x64 // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = 0x65 // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = 0x66 // ???
	ErrUnknownRtreeIndexDistanceType = 0x67 //Unknown RTREE index distance type %s
	ErrProtocol                      = 0x68 // %s
	ErrUpsertUniqueSecondaryKey      = 0x69 // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = 0x6a // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = 0x6b // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = 0x6c // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = 0x6d // Wrong schema version, current: %d, in request: %u
	ErrSlabAllocMax                  = 0x6e // Failed to allocate %u bytes for tuple in the slab allocator: tuple is too large. Check 'slab_alloc_maximal' configuration option.
)

const (
	GreetingSize = 128
)

const (
	ServerIdent = "Tarantool 1.6.8 (Binary)"
)
