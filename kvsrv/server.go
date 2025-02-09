package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

// DPrintf logs debug output when Debug is true.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// KVServer represents the key/value server.
type KVServer struct {
	mu        sync.Mutex
	data      map[string]string // In-memory key-value store.
	lastOp    map[int64]int64   // Last processed opID per client.
	lastReply map[int64]string  // Last reply value for that op per client.
}

// Get handles a Get RPC.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("Get Request: Key=%s, Returning Value=%s", args.Key, kv.data[args.Key])
	reply.Value = kv.data[args.Key]
}

// Put handles a Put RPC.
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Deduplication: if we've already processed this op, return the stored reply.
	if last, ok := kv.lastOp[args.ClientID]; ok && last == args.OpID {
		reply.OldValue = kv.lastReply[args.ClientID]
		return
	}

	// Process the Put operation.
	kv.data[args.Key] = args.Value
	reply.OldValue = "" // Put always returns an empty reply.

	// Record this op for deduplication.
	kv.lastOp[args.ClientID] = args.OpID
	kv.lastReply[args.ClientID] = reply.OldValue
}

// Append handles an Append RPC.
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Deduplication: if we've already processed this op, return the stored reply.
	if last, ok := kv.lastOp[args.ClientID]; ok && last == args.OpID {
		reply.OldValue = kv.lastReply[args.ClientID]
		return
	}

	// Process the Append operation.
	oldValue := kv.data[args.Key]
	newValue := oldValue + args.Value
	kv.data[args.Key] = newValue
	reply.OldValue = oldValue

	// Record this op for deduplication.
	kv.lastOp[args.ClientID] = args.OpID
	kv.lastReply[args.ClientID] = reply.OldValue
}

// StartKVServer creates and initializes a KVServer.
func StartKVServer() *KVServer {
	kv := &KVServer{
		data:      make(map[string]string),
		lastOp:    make(map[int64]int64),
		lastReply: make(map[int64]string),
	}
	return kv
}
