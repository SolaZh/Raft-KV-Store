package kvraft

import (
    "ece419/labgob"
    "ece419/labrpc"
    "ece419/raft"
    "log"
    "sync"
    "sync/atomic"
    "time"
)

const ApplyTimeout = 50 * time.Millisecond // poll interval
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        log.Printf(format, a...)
    }
    return
}

type Op struct {
    // Fields must be capitalized for RPC marshalling.
    Op       string // "Get", "Put", or "Append"
    Key      string
    Value    string
    ClientID int64
    OpID     int64
}

type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg
    dead    int32 // set by Kill()

    maxraftstate int

    // Our key/value store and dedup table:
    store       map[string]string // actual KV storage
    lastApplied map[int64]int64   // dedup: clientID -> lastOpID
    notifyCh    map[int]chan Op   // log index -> channel for notifying an RPC handler
}

func init() {
    labgob.Register(Op{})
}

// --------------------- RPC Handlers ---------------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    op := Op{
        Op:       "Get",
        Key:      args.Key,
        ClientID: args.ClientID,
        OpID:     args.OpID,
    }

    // Start in Raft
    index, startTerm, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    ch := kv.makeNotifyCh(index)

    for {
        select {
        case committedOp := <-ch:
            if committedOp.ClientID == op.ClientID && committedOp.OpID == op.OpID {
                kv.mu.Lock()
                val, found := kv.store[op.Key]
                kv.mu.Unlock()
                if found {
                    reply.Value = val
                } else {
                    reply.Value = ""
                }
                // **Important**: we tell the harness "OK" from a leader perspective:
                reply.Err = OK
            } else {
                reply.Err = ErrWrongLeader
            }
			DPrintf("[KVServer %d %s] returning, reply=%v, index=%d", kv.me, op.Op, reply, index)
            return

        case <-time.After(ApplyTimeout):
            // poll to see if we’re still leader in the same term
            curTerm, stillLeader := kv.rf.GetState()
            if !stillLeader || curTerm != startTerm {
                reply.Err = ErrWrongLeader
				DPrintf("[KVServer %d GET] timed out or lost leadership, reply=%v, index=%d",kv.me, reply, index)
                return
            }
        }
    }
	
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
    op := Op{
        Op:       "Put",
        Key:      args.Key,
        Value:    args.Value,
        ClientID: args.ClientID,
        OpID:     args.OpID,
    }

    // We need the term from Start(op) to detect lost leadership.
    index, startTerm, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    ch := kv.makeNotifyCh(index)

    for {
        select {
        case committedOp := <-ch:
            if committedOp.ClientID == op.ClientID && committedOp.OpID == op.OpID {
                reply.Err = OK
            } else {
                reply.Err = ErrWrongLeader
            }
            return

        case <-time.After(ApplyTimeout):
            // Are we still leader with the same term?
            curTerm, stillLeader := kv.rf.GetState()
            if !stillLeader || curTerm != startTerm {
                reply.Err = ErrWrongLeader
                return
            }
        }
    }
	DPrintf("[KVServer %d %s] returning, reply=%v, index=%d", 
    kv.me, op.Op, reply, index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
    op := Op{
        Op:       "Append",
        Key:      args.Key,
        Value:    args.Value,
        ClientID: args.ClientID,
        OpID:     args.OpID,
    }

    index, startTerm, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    ch := kv.makeNotifyCh(index)

    for {
        select {
        case committedOp := <-ch:
            if committedOp.ClientID == op.ClientID && committedOp.OpID == op.OpID {
                reply.Err = OK
            } else {
                reply.Err = ErrWrongLeader
            }
            return

        case <-time.After(ApplyTimeout):
            curTerm, stillLeader := kv.rf.GetState()
            if !stillLeader || curTerm != startTerm {
                reply.Err = ErrWrongLeader
                return
            }
        }
    }
	DPrintf("[KVServer %d %s] returning, reply=%v, index=%d", 
    kv.me, op.Op, reply, index)
}

// ---------------------- Helpers --------------------------------------

// makeNotifyCh returns the existing channel for a Raft log index or creates a new one.
func (kv *KVServer) makeNotifyCh(index int) chan Op {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if _, ok := kv.notifyCh[index]; !ok {
        kv.notifyCh[index] = make(chan Op, 1)
    }
    return kv.notifyCh[index]
}

// ---------------------- Main apply loop ------------------------------

func (kv *KVServer) applyLoop() {
    for msg := range kv.applyCh {
		DPrintf("[KVServer %d] applyLoop sees msg=%+v", kv.me, msg)
        if kv.killed() {
            return
        }

        if msg.CommandValid {
            op := msg.Command.(Op)

            kv.mu.Lock()
            // Deduplicate if new
            lastOpID, seen := kv.lastApplied[op.ClientID]
            if !seen || op.OpID > lastOpID {
                switch op.Op {
                case "Put":
                    kv.store[op.Key] = op.Value
                case "Append":
                    kv.store[op.Key] += op.Value
                // case "Get":
                    // No store change, but still record the dedup
                }
                kv.lastApplied[op.ClientID] = op.OpID
            }

            if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
                ch <- op
                close(ch)
                delete(kv.notifyCh, msg.CommandIndex)
            }
            kv.mu.Unlock()

        } else if msg.SnapshotValid {
            // handle snapshot if you need to (lab 4C / 4D)
        }
    }
	
}

// ---------------------- Kill & Start ---------------------------------

func (kv *KVServer) Kill() {
    atomic.StoreInt32(&kv.dead, 1)
    kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
    return atomic.LoadInt32(&kv.dead) == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    labgob.Register(Op{})

    kv := &KVServer{}
    kv.me = me
    kv.maxraftstate = maxraftstate
    kv.store = make(map[string]string)
    kv.lastApplied = make(map[int64]int64)
    kv.notifyCh = make(map[int]chan Op)

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go kv.applyLoop()
    return kv
}
