package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string  // "Put" or "Append" (needed to distinguish operations)
    ClientID int64   // Unique identifier for each client
    OpID     int64   // Unique identifier for each operation (prevents duplicate execution)
}

type PutAppendReply struct {
	OldValue string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpID int64
}

type GetReply struct {
	Value string
}
