package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	lock      sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	leaderId int
	role     int //1 follower; 2 candidate; 3 leader;

	heartbeatCh chan struct{}
	hb          func()

	electionTimeout  time.Duration
	heartBeatTimeout time.Duration
	timer            *time.Timer
	ticker           *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.me == rf.leaderId

	fmt.Printf("GetState [id%d][role:%d][leader:%d][term:%d]\n", rf.me, rf.role, rf.leaderId, rf.currentTerm)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntries struct {
	Term     int
	LeaderId int
}

type ReplyAppendEntries struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.killed() {
		return
	}
	if args.CandidateId == rf.me {
		reply.VoteGranted = true
		return
	}
	if rf.role == 3 {
		reply.VoteGranted = false
		return
	}
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		fmt.Printf("[%d] grant [%d]'s vote [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	} else {
		reply.VoteGranted = false
		fmt.Printf("[%d] reject [%d]'s vote [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}
	return
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.killed() {
		return
	}
	if args.LeaderId == rf.me {
		reply.Success = true
		return
	}
	if args.Term >= rf.currentTerm {
		if rf.ticker != nil {
			rf.ticker.Stop()
		}
		rf.role = 1
		rf.votedFor = -1
		if args.Term > rf.currentTerm || rf.leaderId != args.LeaderId {
			fmt.Printf("[id:%d] update term from [%d] to [%d]; leader from [%d] to [%d]\n",
				rf.me, rf.currentTerm, args.Term, rf.leaderId, args.LeaderId)
			rf.leaderId = args.LeaderId
			rf.currentTerm = args.Term
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		go rf.timeout(rf.followerHb)
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	ch := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(rf.heartBeatTimeout):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.me == rf.leaderId

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {

	atomic.StoreInt32(&rf.dead, 1)
	fmt.Printf("[%d] crash\n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = 1
	rf.heartbeatCh = make(chan struct{})
	rf.heartBeatTimeout = 20 * time.Millisecond
	rf.electionTimeout = 150 * time.Millisecond
	rf.votedFor = -1
	rf.leaderId = -1
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.timeout(rf.followerHb)

	fmt.Printf("[node:%d][role:%d][term:%d]start\n", rf.me, rf.role, rf.currentTerm)

	return rf
}

// invoke rf.hb() when timeout
func (rf *Raft) timeout(cb func()) {
	if rf.timer != nil {
		rf.timer.Stop()
	}
	rf.timer = time.NewTimer(time.Duration(rf.calElectionTimeout()) * time.Millisecond)
	select {
	case <-rf.timer.C:
		cb()
	}
}

func (rf *Raft) heartbeat(cb func()) {
	rf.ticker = time.NewTicker(rf.heartBeatTimeout)
	for {
		select {
		case <-rf.ticker.C:
			cb()
		}
	}
}

func (rf *Raft) followerHb() {
	rf.lock.Lock()
	fmt.Printf("[role:%d][id:%d][term:%d] transition to candidate \n",
		rf.role, rf.me, rf.currentTerm)
	rf.leaderId = -1
	rf.votedFor = -1
	rf.role = 2
	rf.lock.Unlock()
	go rf.timeout(rf.candidateHb)
}

func (rf *Raft) candidateHb() {

	rf.lock.Lock()

	if rf.votedFor != -1 {
		fmt.Printf("$2 [id:%d][role:%d][term:%d][votefor:%d] transition to follower\n",
			rf.me, rf.role, rf.currentTerm, rf.votedFor)
		rf.role = 1
		rf.lock.Unlock()
		go rf.timeout(rf.followerHb)
		return
	}
	rf.currentTerm += 1
	rf.votedFor = rf.me

	fmt.Printf("election start [id:%d][role:%d][term:%d][votefor:%d]\n", rf.me, rf.role, rf.currentTerm, rf.votedFor)
	peers := rf.peers
	if len(peers) == 0 {
		rf.role = 3
		rf.lock.Unlock()
		go rf.heartbeat(rf.leaderHb)
		return
	}

	quorum := 1
	voteCount := 0

	rf.lock.Unlock()

	for i := range peers {
		if i == rf.me {
			continue
		}
		go func(j int) {
			rf.lock.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.lock.Unlock()
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(j, &args, &reply)
			fmt.Printf("[%d] send vote to [%d] reply:[%+v]\n", rf.me, j, reply)
			rf.lock.Lock()
			voteCount += 1

			if ok {
				if reply.VoteGranted {
					quorum += 1
				}
			}

			if quorum >= (len(rf.peers)/2 + 1) {
				if rf.role == 3 {
					rf.lock.Unlock()
					return
				}
				fmt.Printf("[id:%d][term:%d] win election transition to leader \n", rf.me, rf.currentTerm)
				rf.leaderId = rf.me
				rf.role = 3
				rf.lock.Unlock()
				go rf.heartbeat(rf.leaderHb)

			} else if voteCount == len(peers)-1 {
				//election complete voteCount be equals to peers count -1
				fmt.Printf("$1 election lose [%d]-transition to follower\n", rf.me)
				rf.role = 1
				rf.lock.Unlock()
				go rf.timeout(rf.followerHb)
				return
			} else {
				rf.lock.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) leaderHb() {
	rf.lock.Lock()
	request := RequestAppendEntries{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.lock.Unlock()
	reply := ReplyAppendEntries{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(j int) {
			rf.sendAppendEntries(j, &request, &reply)
			if reply.Term > rf.currentTerm {
				rf.leaderId = j
				rf.role = 1
				go rf.timeout(rf.followerHb)
			}
		}(i)
	}
}

func (rf *Raft) calElectionTimeout() int64 {
	n := rand.Int63n(rf.electionTimeout.Milliseconds()) + rf.electionTimeout.Milliseconds()
	return n
}
