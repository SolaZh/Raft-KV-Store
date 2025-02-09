package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// main
func main() {
	// start server
	server()

	// client side
	put("419", "cool")
	fmt.Printf("put(419, cool) done\n")
	value, ok := get("419")
	if ok { // should be printed
		fmt.Printf("get(419) -> %s\n", value)
	}
	// issue get() without previous put()
	value, ok = get("41")
	if ok { // should not be printed
		fmt.Printf("get(41) -> %s\n", value)
	}
}

// Client
//
// client creates a TCP connection to the server on each RPC request
func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	return client
}

// define request/reply parameters for each RPC
// parameters should be public, so use capitalized fields
type GetRequest struct {
	Key string
}
type GetReply struct {
	Value string
	Ok    bool
}
type PutRequest struct {
	Key   string
	Value string
}
type PutReply struct {
}

// get client stub function
func get(key string) (string, bool) {
	client := connect()
	args := GetRequest{key}
	reply := GetReply{}
	// Call the server with the procedure name, args, location of reply
	//
	// RPC library:
	// 1) marshalls request args
	// 2) sends request and waits for reply
	// 3) unmarshalls reply
	// 4) returns value from Call() indicates if RPC library got a reply
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error: ", err)
	}
	client.Close()
	return reply.Value, reply.Ok
}

// put client stub function
func put(key string, val string) {
	client := connect()
	args := PutRequest{key, val}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error: ", err)
	}
	client.Close()
}

// Server
type KV struct {
	mu   sync.Mutex
	data map[string]string
}

func server() {
	// initialize kv store
	kv := &KV{data: map[string]string{}}
	// create an RPC server
	rpcServer := rpc.NewServer()
	// register with rpcServer an object (kv)
	// whose methods are RPC handler functions
	rpcServer.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	// run the rest of the server in a separate thread since
	// main() starts server and runs client code
	go func() {
		for {
			conn, err := l.Accept()
			// On new request (conn), ServeConn() in RPC library:
			// 1) reads request
			// 2) unmarshalls request args
			// 3) dispatches (calls) the registered object's method
			// 4) marshalls reply
			// 5) writes reply to connection
			//
		        // ServConn blocks, so create a new thread
		        // for each request. Other options are possible, e.g.,
		        // use a thread pool
			if err == nil {
				go rpcServer.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
}

// Get handler function of kv store
func (kv *KV) Get(args *GetRequest, reply *GetReply) error {
	// use locking since each request is served by a separate thread
	kv.mu.Lock()
	// defer ensures that unlock happens when function returns
	defer kv.mu.Unlock()
	reply.Value, reply.Ok = kv.data[args.Key]
	return nil
}

// Put handler function of kv store
func (kv *KV) Put(args *PutRequest, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
	return nil
}
