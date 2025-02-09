package kvsrv

import "ece419/labrpc"
import "crypto/rand"
import "math/big"
//import "time"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.ClientID = nrand() //assign a unique clientID to this Clerk
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, OpID: nrand()}
	var reply GetReply

	for attempts := 0; attempts < 20; attempts++{
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
		//time.Sleep(time.Duration(1) * 10 * time.Millisecond)
	}
	
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	opID := nrand() // Unique ID for each operation
    args := PutAppendArgs{
        Key:      key,
        Value:    value,
        Op:       op,
        ClientID: ck.ClientID,
        OpID:     opID, 
    }
    var reply PutAppendReply

    for attempts := 0; attempts < 20; attempts++ { // Infinite retry loop
        ok := ck.server.Call("KVServer."+op, &args, &reply) // RPC call
        if ok { // If the request was successful
            return reply.OldValue
        }
		// time.Sleep(time.Duration(1) * 10 * time.Millisecond)
    }
	
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
