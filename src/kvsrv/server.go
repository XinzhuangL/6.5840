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

	data         map[string]*string
	clientMap    map[int64]int
	requestCache map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.clientMap[clientId] == 0 || sequenceId > kv.clientMap[clientId] {
		reply.Value = *kv.data[args.Key]
		kv.requestCache[clientId] = reply.Value
		kv.clientMap[clientId] = sequenceId
	} else {
		reply.Value = kv.requestCache[clientId]
		delete(kv.requestCache, clientId)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.clientMap[clientId] == 0 || sequenceId > kv.clientMap[clientId] {
		oldValue := ""
		if kv.data[args.Key] != nil {
			oldValue = *kv.data[args.Key]
		}
		kv.data[args.Key] = &args.Value
		reply.Value = oldValue
		kv.requestCache[clientId] = args.Value
		kv.clientMap[clientId] = sequenceId
	} else {
		reply.Value = kv.requestCache[clientId]
		delete(kv.requestCache, clientId)
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientId := args.ClientId
	sequenceId := args.SequenceId
	if kv.clientMap[clientId] == 0 || sequenceId > kv.clientMap[clientId] {
		oldValue := ""
		if kv.data[args.Key] != nil {
			oldValue = *kv.data[args.Key]
		}
		newValue := oldValue + args.Value
		kv.data[args.Key] = &newValue
		reply.Value = oldValue
		kv.requestCache[clientId] = oldValue
		kv.clientMap[clientId] = sequenceId
	} else {
		reply.Value = kv.requestCache[clientId]
		delete(kv.requestCache, clientId)
	}

}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data:         make(map[string]*string),
		clientMap:    make(map[int64]int),
		requestCache: make(map[int64]string),
	}
	return kv
}
