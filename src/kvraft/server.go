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

const Debug = 0

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

	originOp := Op{
		Type:  entry.Type,
		Key:   entry.Key,
		Value: entry.Value,
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return ErrWrongLeader
	}

	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return ErrWrongLeader
	}

	ch := kv.putIfAbsent(index)
	//分区后leader无法commit，导致超时
	op := notified(ch)

	if equalOp(originOp, op) {
		return OK
	} else {
		// fake leader, log at index has be overwrite
		return ErrWrongLeader
	}
}

func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.Type == b.Type
}

func notified(ch chan Op) Op {
	select {
	case op := <-ch:
		return op
	case <-time.After(1 * time.Second):
		return Op{}
	}
}

func (kv *KVServer) putIfAbsent(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.commitCh[idx]; !ok {
		kv.commitCh[idx] = make(chan Op, 1)
	}
	return kv.commitCh[idx]
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
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args.Type {
	case "Put":
		kv.db[args.Key] = args.Value
		kv.ack[args.Id] = args.SeqId
	case "Append":
		kv.db[args.Key] += args.Value
		kv.ack[args.Id] = args.SeqId
	case "Get":
	}
}

func (kv *KVServer) listenApplied() {
	go func() {
		for msg := range kv.applyCh {
			op := msg.Command.(Op)
			if !kv.CheckDup(op.Id, op.SeqId) {
				kv.ApplyToKvDb(op)
			}
			ch := kv.putIfAbsent(msg.CommandIndex)
			ch <- op
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.ack = make(map[int]int)
	kv.commitCh = make(map[int]chan Op)

	kv.listenApplied()

	return kv
}
