package kvraft

import (
	"../labrpc"
	"math/rand"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id    int
	seqId int
	mu    sync.Mutex
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = rand.Intn(10)
	ck.seqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{
		Key:   key,
		Id:    ck.id,
		SeqId: ck.seqId,
	}
	reply := GetReply{}
	ck.seqId++

	for {
		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				return reply.Value
			}
		}
		time.Sleep(100)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    ck.id,
		SeqId: ck.seqId,
	}
	reply := PutAppendReply{}
	ck.seqId++

	for {
		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				//DPrintf("write -- key[%s] -- val[%s] -- reply [%+v]", args.Key, args.Value, reply)
				return
			}
		}
		time.Sleep(100)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
