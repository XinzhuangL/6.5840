package kvsrv

import (
	"log"
	"sync"
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

	data map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]
	log.Printf("[kv] Get %v %v", args.Key, len(reply.Value))
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = oldValue
	log.Printf("[kv] Put %v %v to %v", args.Key, len(oldValue), len(kv.data[args.Key]))
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	log.Printf("[kv] Append %v %v + %v == %v", args.Key, len(oldValue), len(args.Value), len(kv.data[args.Key]))
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]string),
	}
	return kv
}
