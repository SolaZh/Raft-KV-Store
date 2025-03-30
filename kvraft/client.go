package kvraft

import (
    "crypto/rand"
    "ece419/labrpc"
    "math/big"
    "sync"
	// "fmt"
)

// Clerk represents a client that talks to our KV service.
type Clerk struct {
    mu             sync.Mutex
    servers        []*labrpc.ClientEnd
    ClientID       int64
    nextOpNum      int64  // increments for each request
    assumedLeader  int
}

// generate a random 64-bit number, typically used for unique client IDs.
func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    return bigx.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := &Clerk{}
    ck.servers = servers
    ck.ClientID = nrand() // unique client ID
    ck.assumedLeader = 0
    ck.nextOpNum = 0
	// fmt.Printf("DEBUG: MakeClerk called - new Clerk with ClientID=%d\n", ck.ClientID)
    return ck
}

// Get fetches the current value for a key, retrying indefinitely on errors.
func (ck *Clerk) Get(key string) string {
    ck.mu.Lock()
    opNum := ck.nextOpNum
    ck.nextOpNum++
    ck.mu.Unlock()
	// fmt.Printf("DEBUG: Clerk generating Get opNum=%d for key=%s\n", opNum, key)
    args := GetArgs{
        Key:      key,
        ClientID: ck.ClientID,
        OpID:     opNum, // only assignment
    }

    for {
        var reply GetReply
        ok := ck.servers[ck.assumedLeader].Call("KVServer.Get", &args, &reply)
        if ok && reply.Err == OK {
            return reply.Value
        } else if ok && reply.Err == ErrWrongLeader {
            ck.assumedLeader = (ck.assumedLeader + 1) % len(ck.servers)
        } else {
            ck.assumedLeader = (ck.assumedLeader + 1) % len(ck.servers)
        }
    }
	
}

// PutAppend handles both Put and Append (op = "Put" or "Append").
func (ck *Clerk) PutAppend(key string, value string, op string) {
    ck.mu.Lock()
    opNum := ck.nextOpNum
    ck.nextOpNum++
    ck.mu.Unlock()

    args := PutAppendArgs{
        Key:      key,
        Value:    value,
        Op:       op,
        ClientID: ck.ClientID,
        OpID:     opNum,
    }

    for {
        var reply PutAppendReply
        var ok bool
        if op == "Put" {
            ok = ck.servers[ck.assumedLeader].Call("KVServer.Put", &args, &reply)
        } else {
            ok = ck.servers[ck.assumedLeader].Call("KVServer.Append", &args, &reply)
        }

        if ok && reply.Err == OK {
            // success
            return
        } else if ok && reply.Err == ErrWrongLeader {
            ck.assumedLeader = (ck.assumedLeader + 1) % len(ck.servers)
        } else {
            ck.assumedLeader = (ck.assumedLeader + 1) % len(ck.servers)
        }
        // retry
    }
}

// Put and Append are just convenience wrappers.
func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}
