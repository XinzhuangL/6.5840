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

	data         map[string]string
	requestCache map[int64]map[int64]*string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.requestCache[clientId] != nil && kv.requestCache[clientId][sequenceId] != nil {
		reply.Value = *kv.requestCache[clientId][sequenceId]
	} else {
		reply.Value = kv.data[args.Key]
		if kv.requestCache[clientId] == nil {
			kv.requestCache[clientId] = make(map[int64]*string)
		}
		kv.requestCache[clientId][sequenceId] = &reply.Value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.requestCache[clientId] != nil && kv.requestCache[clientId][sequenceId] != nil {
		reply.Value = *kv.requestCache[clientId][sequenceId]
	} else {
		oldValue := kv.data[args.Key]
		kv.data[args.Key] = args.Value
		reply.Value = oldValue
		if kv.requestCache[clientId] == nil {
			kv.requestCache[clientId] = make(map[int64]*string)
		}
		value, _ := kv.data[args.Key]
		kv.requestCache[clientId][sequenceId] = &value
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.requestCache[clientId] != nil && kv.requestCache[clientId][sequenceId] != nil {
		reply.Value = *kv.requestCache[clientId][sequenceId]
	} else {
		oldValue := kv.data[args.Key]
		kv.data[args.Key] = oldValue + args.Value
		reply.Value = oldValue
		if kv.requestCache[clientId] == nil {
			kv.requestCache[clientId] = make(map[int64]*string)
		}
		kv.requestCache[clientId][sequenceId] = &oldValue
	}

}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data:         make(map[string]string),
		requestCache: make(map[int64]map[int64]*string),
	}
	return kv
}
