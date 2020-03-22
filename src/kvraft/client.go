package kvraft

import (
	"../labrpc"
	"fmt"
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
	ck.id = generateRandomNumber(10, 1000000, 1)[0]
	fmt.Println(ck.id)
	ck.seqId = 0
	return ck
}

//生成若干个不重复的随机数
func RandomTestBase() {
	//测试5次
	for i := 0; i < 5; i++ {
		nums := generateRandomNumber(10, 30, 10)
		fmt.Println(nums)
	}
}

//生成count个[start,end)结束的不重复的随机数
func generateRandomNumber(start int, end int, count int) []int {
	//范围检查
	if end < start || (end-start) < count {
		return nil
	}
	//存放结果的slice
	nums := make([]int, 0)
	//随机数生成器，加入时间戳保证每次生成的随机数不一样
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		//生成随机数
		num := r.Intn((end - start)) + start
		//查重
		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}
		if !exist {
			nums = append(nums, num)
		}
	}
	return nums
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

			ok := ck.call(func() bool {
				return ck.servers[i].Call("KVServer.Get", &args, &reply)
			})
			if ok && reply.Err == OK {
				DPrintf("READ -- [%d] -- args[%+v] -- reply [%+v]", i, args, reply)
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

			ok := ck.call(func() bool {
				return ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			})
			if ok && reply.Err == OK {
				DPrintf("WRITE -- [%d] -- args[%+v] -- val[%s] -- reply [%+v]", i, args, args.Value, reply)
				return
			}
		}
		time.Sleep(100)
	}
}

func (ck *Clerk) call(fn func() bool) bool {

	ch := make(chan bool)

	go func() {
		ok := fn()
		ch <- ok
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(250 * time.Second):
		return false
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
