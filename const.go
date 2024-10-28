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
	KeyVersionID      = uint(0x06)
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
	ErrUnknown                       = uint(0x00)  // Unknown error
	ErrIllegalParams                 = uint(0x01)  // Illegal parameters, %s
	ErrMemoryIssue                   = uint(0x02)  // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = uint(0x03)  // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = uint(0x04)  // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = uint(0x05)  // %s does not support %s
	ErrNonmaster                     = uint(0x06)  // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = uint(0x07)  // Can't modify data because this server is in read-only mode.
	ErrInjection                     = uint(0x08)  // Error injection '%s'
	ErrCreateSpace                   = uint(0x09)  // Failed to create space '%s': %s
	ErrSpaceExists                   = uint(0x0a)  // Space '%s' already exists
	ErrDropSpace                     = uint(0x0b)  // Can't drop space '%s': %s
	ErrAlterSpace                    = uint(0x0c)  // Can't modify space '%s': %s
	ErrIndexType                     = uint(0x0d)  // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = uint(0x0e)  // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = uint(0x0f)  // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = uint(0x10)  // Tuple format limit reached: %u
	ErrDropPrimaryKey                = uint(0x11)  // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = uint(0x12)  // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = uint(0x13)  // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = uint(0x14)  // Invalid MsgPack - %s
	ErrProcRet                       = uint(0x15)  // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = uint(0x16)  // Tuple/Key must be MsgPack array
	ErrFieldType                     = uint(0x17)  // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = uint(0x18)  // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = uint(0x19)  // SPLICE error on field %u: %s
	ErrArgType                       = uint(0x1a)  // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = uint(0x1b)  // Tuple is too long %u
	ErrUnknownUpdateOp               = uint(0x1c)  // Unknown UPDATE operation
	ErrUpdateField                   = uint(0x1d)  // Field %u UPDATE error: %s
	ErrFiberStack                    = uint(0x1e)  // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = uint(0x1f)  // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = uint(0x20)  // %s
	ErrNoSuchProc                    = uint(0x21)  // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = uint(0x22)  // Trigger is not found
	ErrNoSuchIndex                   = uint(0x23)  // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = uint(0x24)  // Space '%s' does not exist
	ErrNoSuchField                   = uint(0x25)  // Field %d was not found in the tuple
	ErrSpaceFieldCount               = uint(0x26)  // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = uint(0x27)  // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = uint(0x28)  // Failed to write to disk
	ErrMoreThanOneTuple              = uint(0x29)  // More than one tuple found by get()
	ErrAccessDenied                  = uint(0x2a)  // %s access denied for user '%s'
	ErrCreateUser                    = uint(0x2b)  // Failed to create user '%s': %s
	ErrDropUser                      = uint(0x2c)  // Failed to drop user '%s': %s
	ErrNoSuchUser                    = uint(0x2d)  // User '%s' is not found
	ErrUserExists                    = uint(0x2e)  // User '%s' already exists
	ErrCredsMismatch                 = uint(0x2f)  // User not found or supplied credentials are invalid
	ErrUnknownRequestType            = uint(0x30)  // Unknown request type %u
	ErrUnknownSchemaObject           = uint(0x31)  // Unknown object type '%s'
	ErrCreateFunction                = uint(0x32)  // Failed to create function '%s': %s
	ErrNoSuchFunction                = uint(0x33)  // Function '%s' does not exist
	ErrFunctionExists                = uint(0x34)  // Function '%s' already exists
	ErrFunctionAccessDenied          = uint(0x35)  // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = uint(0x36)  // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = uint(0x37)  // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = uint(0x38)  // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = uint(0x39)  // Space engine '%s' does not exist
	ErrReloadCfg                     = uint(0x3a)  // Can't set option '%s' dynamically
	ErrCfg                           = uint(0x3b)  // Incorrect value for option '%s': %s
	ErrSophia                        = uint(0x3c)  // %s
	ErrLocalServerIsNotActive        = uint(0x3d)  // Local server is not active
	ErrUnknownServer                 = uint(0x3e)  // Server %s is not registered with the cluster
	ErrClusterIDMismatch             = uint(0x3f)  // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUUID                   = uint(0x40)  // Invalid UUID: %s
	ErrClusterIDIsRo                 = uint(0x41)  // Can't reset cluster id: it is already assigned
	ErrReserved66                    = uint(0x42)  // Reserved66
	ErrServerIDIsReserved            = uint(0x43)  // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = uint(0x44)  // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = uint(0x45)  // Missing mandatory field '%s' in request
	ErrIdentifier                    = uint(0x46)  // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = uint(0x47)  // Can't drop function %u: %s
	ErrIteratorType                  = uint(0x48)  // Unknown iterator type '%s'
	ErrReplicaMax                    = uint(0x49)  // Replica count limit reached: %u
	ErrInvalidXlog                   = uint(0x4a)  // Failed to read xlog: %lld
	ErrInvalidXlogName               = uint(0x4b)  // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = uint(0x4c)  // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = uint(0x4d)  // Connection is not established
	ErrTimeout                       = uint(0x4e)  // Timeout exceeded
	ErrActiveTransaction             = uint(0x4f)  // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = uint(0x50)  // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = uint(0x51)  // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = uint(0x52)  // Role '%s' is not found
	ErrRoleExists                    = uint(0x53)  // Role '%s' already exists
	ErrCreateRole                    = uint(0x54)  // Failed to create role '%s': %s
	ErrIndexExists                   = uint(0x55)  // Index '%s' already exists
	ErrTupleRefOverflow              = uint(0x56)  // Tuple reference counter overflow
	ErrRoleLoop                      = uint(0x57)  // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = uint(0x58)  // Incorrect grant arguments: %s
	ErrPrivGranted                   = uint(0x59)  // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = uint(0x5a)  // User '%s' already has role '%s'
	ErrPrivNotGranted                = uint(0x5b)  // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = uint(0x5c)  // User '%s' does not have role '%s'
	ErrMissingSnapshot               = uint(0x5d)  // Can't find snapshot
	ErrCantUpdatePrimaryKey          = uint(0x5e)  // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = uint(0x5f)  // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = uint(0x60)  // Setting password for guest user has no effect
	ErrTransactionConflict           = uint(0x61)  // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = uint(0x62)  // Unsupported role privilege '%s'
	ErrLoadFunction                  = uint(0x63)  // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = uint(0x64)  // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = uint(0x65)  // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = uint(0x66)  // ???
	ErrUnknownRtreeIndexDistanceType = uint(0x67)  //Unknown RTREE index distance type %s
	ErrProtocol                      = uint(0x68)  // %s
	ErrUpsertUniqueSecondaryKey      = uint(0x69)  // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = uint(0x6a)  // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = uint(0x6b)  // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = uint(0x6c)  // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = uint(0x6d)  // Wrong schema version, current: %d, in request: %u
	ErrMemtxMaxTupleSize             = uint(0x6e)  // "Failed to allocate %u bytes for tuple: tuple is too large. Check 'memtx_max_tuple_size' configuration option."
	ErrWrongSpaceOptions             = uint(0x6f)  // "Wrong space options: %s"
	ErrUnsupportedIndexFeature       = uint(0x70)  // "Index '%s' (%s) of space '%s' (%s) does not support %s"
	ErrViewIsRo                      = uint(0x71)  // "View '%s' is read-only"
	ErrNoTransaction                 = uint(0x72)  // "No active transaction"
	ErrSystem                        = uint(0x73)  // "%s"
	ErrLoading                       = uint(0x74)  // "Instance bootstrap hasn't finished yet"
	ErrConnectionToSelf              = uint(0x75)  // "Connection to self"
	ErrKeyPartIsTooLong              = uint(0x76)  // "Key part is too long: %u of %u bytes"
	ErrCompression                   = uint(0x77)  // "Compression error: %s"
	ErrCheckpointInProgress          = uint(0x78)  // "Snapshot is already in progress"
	ErrSubStmtMax                    = uint(0x79)  // "Can not execute a nested statement: nesting limit reached"
	ErrCommitInSubStmt               = uint(0x7a)  // "Can not commit transaction in a nested statement"
	ErrRollbackInSubStmt             = uint(0x7b)  // "Rollback called in a nested statement"
	ErrDecompression                 = uint(0x7c)  // "Decompression error: %s"
	ErrInvalidXlogType               = uint(0x7d)  // "Invalid xlog type: expected %s, got %s"
	ErrAlreadyRunning                = uint(0x7e)  // "Failed to lock WAL directory %s and hot_standby mode is off"
	ErrIndexFieldCountLimit          = uint(0x7f)  // "Indexed field count limit reached: %d indexed fields"
	ErrLocalInstanceIdIsReadOn       = uint(0x80)  // "The local instance id %u is read-only"ly
	ErrBackupInProgress              = uint(0x81)  // "Backup is already in progress"
	ErrReadViewAborted               = uint(0x82)  // "The read view is aborted"
	ErrInvalidIndexFile              = uint(0x83)  // "Invalid INDEX file %s: %s"
	ErrInvalidRunFile                = uint(0x84)  // "Invalid RUN file: %s"
	ErrInvalidVylogFile              = uint(0x85)  // "Invalid VYLOG file: %s"
	ErrCascadeRollback               = uint(0x86)  // "WAL has a rollback in progress"
	ErrVyQuotaTimeout                = uint(0x87)  // "Timed out waiting for Vinyl memory quota"
	ErrPartialKey                    = uint(0x88)  // "%s index  does not support selects via a partial key (expected %u parts, got %u). Please Consider changing index type to TREE."
	ErrTruncateSystemSpace           = uint(0x89)  // "Can't truncate a system space, space '%s'"
	ErrLoadModule                    = uint(0x8a)  // "Failed to dynamically load module '%.*s': %s"
	ErrVinylMaxTupleSize             = uint(0x8b)  // "Failed to allocate %u bytes for tuple: tuple is too large. Check 'vinyl_max_tuple_size' configuration option."
	ErrWrongDdVersion                = uint(0x8c)  // "Wrong _schema version: expected 'major.minor[.patch]'"
	ErrWrongSpaceFormat              = uint(0x8d)  // "Wrong space format field %u: %s"
	ErrCreateSequence                = uint(0x8e)  // "Failed to create sequence '%s': %s"
	ErrAlterSequence                 = uint(0x8f)  // "Can't modify sequence '%s': %s"
	ErrDropSequence                  = uint(0x90)  // "Can't drop sequence '%s': %s"
	ErrNoSuchSequence                = uint(0x91)  // "Sequence '%s' does not exist"
	ErrSequenceExists                = uint(0x92)  // "Sequence '%s' already exists"
	ErrSequenceOverflow              = uint(0x93)  // "Sequence '%s' has overflowed"
	ErrNoSuchIndexName               = uint(0x94)  // "No index '%s' is defined in space '%s'"
	ErrSpaceFieldIsDuplicate         = uint(0x95)  // "Space field '%s' is duplicate"
	ErrCantCreateCollation           = uint(0x96)  // "Failed to initialize collation: %s."
	ErrWrongCollationOptions         = uint(0x97)  // "Wrong collation options: %s"
	ErrNullablePrimary               = uint(0x98)  // "Primary index of space '%s' can not contain nullable parts"
	ErrNoSuchFieldNameInSpace        = uint(0x99)  // "Field '%s' was not found in space '%s' format"
	ErrTransactionYield              = uint(0x9a)  // "Transaction has been aborted by a fiber yield"
	ErrNoSuchGroup                   = uint(0x9b)  // "Replication group '%s' does not exist"
	ErrSqlBindValue                  = uint(0x9c)  // "Bind value for parameter %s is out of range for type %s"
	ErrSqlBindType                   = uint(0x9d)  // "Bind value type %s for parameter %s is not supported"
	ErrSqlBindParameterMax           = uint(0x9e)  // "SQL bind parameter limit reached: %d"
	ErrSqlExecute                    = uint(0x9f)  // "Failed to execute SQL statement: %s"
	ErrUpdateDecimalOverflow         = uint(0xa0)  // "Decimal overflow when performing operation '%c' on field %s"
	ErrSqlBindNotFound               = uint(0xa1)  // "Parameter %s was not found in the statement"
	ErrActionMismatch                = uint(0xa2)  // "Field %s contains %s on conflict action, but %s in index parts"
	ErrViewMissingSql                = uint(0xa3)  // "Space declared as a view must have SQL statement"
	ErrForeignKeyConstraint          = uint(0xa4)  // "Can not commit transaction: deferred foreign keys violations are not resolved"
	ErrNoSuchModule                  = uint(0xa5)  // "Module '%s' does not exist"
	ErrNoSuchCollation               = uint(0xa6)  // "Collation '%s' does not exist"
	ErrCreateFkConstraint            = uint(0xa7)  // "Failed to create foreign key constraint '%s': %s"
	ErrDropFkConstraint              = uint(0xa8)  // "Failed to drop foreign key constraint '%s': %s"
	ErrNoSuchConstraint              = uint(0xa9)  // "Constraint '%s' does not exist in space '%s'"
	ErrConstraintExists              = uint(0xaa)  // "%s constraint '%s' already exists in space '%s'"
	ErrSqlTypeMismatch               = uint(0xab)  // "Type mismatch: can not convert %s to %s"
	ErrRowidOverflow                 = uint(0xac)  // "Rowid is overflowed: too many entries in ephemeral space"
	ErrDropCollation                 = uint(0xad)  // "Can't drop collation %s : %s"
	ErrIllegalCollationMix           = uint(0xae)  // "Illegal mix of collations"
	ErrSqlNoSuchPragma               = uint(0xaf)  // "Pragma '%s' does not exist"
	ErrSqlCantResolveField           = uint(0xb0)  // "Can’t resolve field '%s'"
	ErrIndexExistsInSpace            = uint(0xb1)  // "Index '%s' already exists in space '%s'"
	ErrInconsistentTypes             = uint(0xb2)  // "Inconsistent types: expected %s got %s"
	ErrSqlSyntaxWithPos              = uint(0xb3)  // "Syntax error at line %d at or near position %d: %s"
	ErrSqlStackOverflow              = uint(0xb4)  // "Failed to parse SQL statement: parser stack limit reached"
	ErrSqlSelectWildcard             = uint(0xb5)  // "Failed to expand '*' in SELECT statement without FROM clause"
	ErrSqlStatementEmpty             = uint(0xb6)  // "Failed to execute an empty SQL statement"
	ErrSqlKeywordIsReserved          = uint(0xb7)  // "At line %d at or near position %d: keyword '%.*s' is reserved. Please use double quotes if '%.*s' is an identifier."
	ErrSqlSyntaxNearToken            = uint(0xb8)  // "Syntax error at line %d near '%.*s'"
	ErrSqlUnknownToken               = uint(0xb9)  // "At line %d at or near position %d: unrecognized token '%.*s'"
	ErrSqlParserGeneric              = uint(0xba)  // "%s"
	ErrSqlAnalyzeArgument            = uint(0xbb)  // "ANALYZE statement argument %s is not a base table"
	ErrSqlColumnCountMax             = uint(0xbc)  // "Failed to create space '%s': space column count %d exceeds the limit (%d)"
	ErrHexLiteralMax                 = uint(0xbd)  // "Hex literal %s%s length %d exceeds the supported limit (%d)"
	ErrIntLiteralMax                 = uint(0xbe)  // "Integer literal %s%s exceeds the supported range [-9223372036854775808, 18446744073709551615]"
	ErrSqlParserLimit                = uint(0xbf)  // "%s %d exceeds the limit (%d)"
	ErrIndexDefUnsupported           = uint(0xc0)  // "%s are prohibited in an index definition"
	ErrCkDefUnsupported              = uint(0xc1)  // "%s are prohibited in a ck constraint definition"
	ErrMultikeyIndexMismatch         = uint(0xc2)  // "Field %s is used as multikey in one index and as single key in another"
	ErrCreateCkConstraint            = uint(0xc3)  // "Failed to create check constraint '%s': %s"
	ErrCkConstraintFailed            = uint(0xc4)  // "Check constraint failed '%s': %s"
	ErrSqlColumnCount                = uint(0xc5)  // "Unequal number of entries in row expression: left side has %u, but right side - %u"
	ErrFuncIndexFunc                 = uint(0xc6)  // "Failed to build a key for functional index '%s' of space '%s': %s"
	ErrFuncIndexFormat               = uint(0xc7)  // "Key format doesn't match one defined in functional index '%s' of space '%s': %s"
	ErrFuncIndexParts                = uint(0xc8)  // "Wrong functional index definition: %s"
	ErrNoSuchFieldName               = uint(0xc9)  // "Field '%s' was not found in the tuple"
	ErrFuncWrongArgCount             = uint(0xca)  // "Wrong number of arguments is passed to %s(): expected %s, got %d"
	ErrBootstrapReadonly             = uint(0xcb)  // "Trying to bootstrap a local read-only instance as master"
	ErrSqlFuncWrongRetCount          = uint(0xcc)  // "SQL expects exactly one argument returned from %s, got %d"
	ErrFuncInvalidReturnType         = uint(0xcd)  // "Function '%s' returned value of invalid type: expected %s got %s"
	ErrSqlParserGenericWithPos       = uint(0xce)  // "At line %d at or near position %d: %s"
	ErrReplicaNotAnon                = uint(0xcf)  // "Replica '%s' is not anonymous and cannot register."
	ErrCannotRegister                = uint(0xd0)  // "Couldn't find an instance to register this replica on."
	ErrSessionSettingInvalidVa       = uint(0xd1)  // "Session setting %s expected a value of type %s"lue
	ErrSqlPrepare                    = uint(0xd2)  // "Failed to prepare SQL statement: %s"
	ErrWrongQueryId                  = uint(0xd3)  // "Prepared statement with id %u does not exist"
	ErrSequenceNotStarted            = uint(0xd4)  // "Sequence '%s' is not started"
	ErrNoSuchSessionSetting          = uint(0xd5)  // "Session setting %s doesn't exist"
	ErrUncommittedForeignSyncT       = uint(0xd6)  // "Found uncommitted sync transactions from other instance with id %u"xns
	ErrSyncMasterMismatch            = uint(0xd7)  // "CONFIRM message arrived for an unknown master id %d, expected %d"
	ErrSyncQuorumTimeout             = uint(0xd8)  // "Quorum collection for a synchronous transaction is timed out"
	ErrSyncRollback                  = uint(0xd9)  // "A rollback for a synchronous transaction is received"
	ErrTupleMetadataIsTooBig         = uint(0xda)  // "Can't create tuple: metadata size %u is too big"
	ErrXlogGap                       = uint(0xdb)  // "Missing .xlog file between LSN %lld %s and %lld %s"
	ErrTooEarlySubscribe             = uint(0xdc)  // "Can't subscribe non-anonymous replica %s until join is done"
	ErrSqlCantAddAutoinc             = uint(0xdd)  // "Can't add AUTOINCREMENT: space %s can't feature more than one AUTOINCREMENT field"
	ErrQuorumWait                    = uint(0xde)  // "Couldn't wait for quorum %d: %s"
	ErrInterferingPromote            = uint(0xdf)  // "Instance with replica id %u was promoted first"
	ErrElectionDisabled              = uint(0xe0)  // "Elections were turned off"
	ErrTxnRollback                   = uint(0xe1)  // "Transaction was rolled back"
	ErrNotLeader                     = uint(0xe2)  // "The instance is not a leader. New leader is %u"
	ErrSyncQueueUnclaimed            = uint(0xe3)  // "The synchronous transaction queue doesn't belong to any instance"
	ErrSyncQueueForeign              = uint(0xe4)  // "The synchronous transaction queue belongs to other instance with id %u"
	ErrUnableToProcessInStream       = uint(0xe5)  // "Unable to process %s request in stream"
	ErrUnableToProcessOutOfStr       = uint(0xe6)  // "Unable to process %s request out of stream"eam
	ErrTransactionTimeout            = uint(0xe7)  // "Transaction has been aborted by timeout"
	ErrActiveTimer                   = uint(0xe8)  // "Operation is not permitted if timer is already running"
	ErrTupleFieldCountLimit          = uint(0xe9)  // "Tuple field count limit reached: see box.schema.FIELD_MAX"
	ErrCreateConstraint              = uint(0xea)  // "Failed to create constraint '%s' in space '%s': %s"
	ErrFieldConstraintFailed         = uint(0xeb)  // "Check constraint '%s' failed for field '%s'"
	ErrTupleConstraintFailed         = uint(0xec)  // "Check constraint '%s' failed for tuple"
	ErrCreateForeignKey              = uint(0xed)  // "Failed to create foreign key '%s' in space '%s': %s"
	ErrForeignKeyIntegrity           = uint(0xee)  // "Foreign key '%s' integrity check failed: %s"
	ErrFieldForeignKeyFailed         = uint(0xef)  // "Foreign key constraint '%s' failed for field '%s': %s"
	ErrComplexForeignKeyFailed       = uint(0xf0)  // "Foreign key constraint '%s' failed: %s"
	ErrWrongSpaceUpgradeOption       = uint(0xf1)  // "Wrong space upgrade options: %s"s
	ErrNoElectionQuorum              = uint(0xf2)  // "Not enough peers connected to start elections: %d out of minimal required %d"
	ErrSsl                           = uint(0xf3)  // "%s"
	ErrSplitBrain                    = uint(0xf4)  // "Split-Brain discovered: %s"
	ErrOldTerm                       = uint(0xf5)  // "The term is outdated: old - %llu, new - %llu"
	ErrInterferingElections          = uint(0xf6)  // "Interfering elections started"
	ErrIteratorPosition              = uint(0xf7)  // "Iterator position is invalid"
	ErrDefaultValueType              = uint(0xf8)  // "Type of the default value does not match tuple field %s type: expected %s, got %s"
	ErrUnknownAuthMethod             = uint(0xf9)  // "Unknown authentication method '%s'"
	ErrInvalidAuthData               = uint(0xfa)  // "Invalid '%s' data: %s"
	ErrInvalidAuthRequest            = uint(0xfb)  // "Invalid '%s' request: %s"
	ErrWeakPassword                  = uint(0xfc)  // "Password doesn't meet security requirements: %s"
	ErrOldPassword                   = uint(0xfd)  // "Password must differ from last %d passwords"
	ErrNoSuchSession                 = uint(0xfe)  // "Session %llu does not exist"
	ErrWrongSessionType              = uint(0xff)  // "Session '%s' is not supported"
	ErrPasswordExpired               = uint(0x100) // "Password expired"
	ErrAuthDelay                     = uint(0x101) // "Too many authentication attempts"
	ErrAuthRequired                  = uint(0x102) // "Authentication required"
	ErrSqlSeqScan                    = uint(0x103) // "Scanning is not allowed for %s"
	ErrNoSuchEvent                   = uint(0x104) // "Unknown event %s"
	ErrBootstrapNotUnanimous         = uint(0x105) // "Replica %s chose a different bootstrap leader %s"
	ErrCantCheckBootstrapLeade       = uint(0x106) // "Can't check who replica %s chose its bootstrap leader"r
	ErrBootstrapConnectionNotT       = uint(0x107) // "Some replica set members were not specified in box.cfg.replication"oAll
	ErrNilUuid                       = uint(0x108) // "Nil UUID is reserved and can't be used in replication"
	ErrWrongFunctionOptions          = uint(0x109) // "Wrong function options: %s"
	ErrMissingSystemSpaces           = uint(0x10a) // "Snapshot has no system spaces"
	ErrClusterNameMismatch           = uint(0x10b) // "Cluster name mismatch: expected %s, got %s"
	ErrReplicasetNameMismatch        = uint(0x10c) // "Replicaset name mismatch: expected %s, got %s"
	ErrInstanceNameDuplicate         = uint(0x10d) // "Duplicate replica name %s, already occupied by %s"
	ErrInstanceNameMismatch          = uint(0x10e) // "Instance name mismatch: expected %s, got %s"
	ErrSchemaNeedsUpgrade            = uint(0x10f) // "Your schema version is %u.%u.%u while Tarantool %s requires a more recent schema version. Please, consider using box.schema.upgrade()."
	ErrSchemaUpgradeInProgress       = uint(0x110) // "Schema upgrade is already in progress"
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
	version2_5_1  = uint32(132353) // VersionID(2, 5, 1)
	version2_8_0  = uint32(133120) // VersionID(2, 8, 0)
	version2_9_0  = uint32(133376) // VersionID(2, 9, 0)
	version2_11_0 = uint32(133888) // VersionID(2, 11, 0)
)
