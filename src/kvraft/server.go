package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // Put / Append / Get
	Key   string
	Value string
	Id    int
	SeqId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       map[string]string
	ack      map[int]int
	commitCh map[int]chan Op
}

func (kv *KVServer) commitEntryLog(entry Op) Err {

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	ch, ok := kv.commitCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.commitCh[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		if op != entry {
			return ErrUnexpected
		}
	case <-time.After(10 * time.Second):
		//fake leader
		log.Println("timeout2")
		return ErrWrongLeader
	}

	delete(kv.commitCh, index)

	return OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	entry := Op{
		Type:  "Get",
		Key:   args.Key,
		Id:    args.Id,
		SeqId: args.SeqId,
	}

	err := kv.commitEntryLog(entry)
	if err == OK {
		reply.Value = kv.db[args.Key]
	}

	reply.Err = err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("PutAppendArgs [%+v]", args)
	entry := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		Id:    args.Id,
		SeqId: args.SeqId,
	}

	//DPrintf("command %v", command)

	err := kv.commitEntryLog(entry)
	reply.Err = err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) CheckDup(id int, seqId int) bool {
	v, ok := kv.ack[id]
	if ok {
		return v >= seqId
	}
	return false
}

func (kv *KVServer) ApplyToKvDb(args Op) {
	switch args.Type {
	case "Put":
		kv.db[args.Key] = args.Value
		kv.ack[args.Id] = args.SeqId
		//DPrintf("Put ckId[%d] -- seqId[%d] -- key[%s] -- val[%s]", args.Id, args.SeqId, args.Key, kv.db[args.Key])
	case "Append":
		kv.db[args.Key] += args.Value
		kv.ack[args.Id] = args.SeqId
		//DPrintf("Append ckId[%d] -- seqId[%d] -- key[%s] -- val[%s]", args.Id, args.SeqId, args.Key, kv.db[args.Key])
	case "Get":
		//DPrintf("Get ckId[%d] -- seqId[%d] -- key[%s] -- val[%s]", args.Id, args.SeqId, args.Key, kv.db[args.Key])
	}
}

func (kv *KVServer) listenApplied() {
	go func() {
		for {
			msg := <-kv.applyCh
			DPrintf("3A -- applied -- [%+v]", msg)
			op := msg.Command.(Op)

			if !kv.CheckDup(op.Id, op.SeqId) {
				kv.ApplyToKvDb(op)
				kv.mu.Lock()
				ch, ok := kv.commitCh[msg.CommandIndex]
				if !ok {
					ch = make(chan Op, 1)
					kv.commitCh[msg.CommandIndex] = ch
				}
				ch <- op
				kv.mu.Unlock()
			}

		}
	}()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.ack = make(map[int]int)
	kv.commitCh = make(map[int]chan Op)

	kv.listenApplied()

	return kv
}
