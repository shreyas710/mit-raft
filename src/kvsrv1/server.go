package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string
	version map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]string)
	kv.version = make(map[string]rpc.Tversion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.data[args.Key]
	if ok {
		reply.Value = value
		reply.Version = kv.version[args.Key]
		reply.Err = rpc.OK
		return
	}
	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ver, exists := kv.version[args.Key]
	if !exists {
		// Key doesn't exist: only allow if version is 0
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.data[args.Key] = args.Value
		kv.version[args.Key] = 1
		reply.Err = rpc.OK
		return
	}
	// Key exists: check version match
	if args.Version != ver {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.data[args.Key] = args.Value
	kv.version[args.Key] = ver + 1
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
