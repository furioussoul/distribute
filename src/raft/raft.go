package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
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

	hb func()

	electionTimeout   time.Duration
	heartBeatInterval time.Duration
	timer             *time.Timer
	ticker            *time.Ticker

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	updateTime  time.Time
	applyCh     chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock.Lock()
	index := -1
	term := -1
	isLeader := rf.me == rf.leaderId

	// Your code here (2B).

	if isLeader {
		prev, entry := rf.appendLog(command)

		request := RequestAppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      []LogEntry{entry},
			PrevLogIndex: prev.Index,
			PrevLogTerm:  prev.Term,
			LeaderCommit: rf.commitIndex,
		}
		reply := ReplyAppendEntries{}
		index = entry.Index
		term = entry.Term
		rf.append(request, reply)
	}

	rf.lock.Unlock()

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
	DPrintf("[%d] crash\n", rf.me)
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
	rf.applyCh = applyCh
	rf.role = 1
	rf.heartBeatInterval = 20 * time.Millisecond
	rf.electionTimeout = 150 * time.Millisecond
	rf.votedFor = -1
	rf.leaderId = -1
	rf.updateTime = time.Now()
	rf.log = []LogEntry{{0, 0, nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.transitionToFollower()

	DPrintf("[node:%d][role:%d][term:%d]start\n", rf.me, rf.role, rf.currentTerm)

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
	rf.ticker = time.NewTicker(rf.heartBeatInterval)
	for {
		select {
		case <-rf.ticker.C:
			cb()
		}
	}
}

func (rf *Raft) followerHb() {
	rf.role = 1
	go rf.timeout(rf.candidateHb)
}

func (rf *Raft) candidateHb() {

	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.role = 2
	rf.votedFor = rf.me
	rf.updateTime = time.Now()
	DPrintf("[%v]-[%d] update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, rf.currentTerm+1)
	rf.currentTerm += 1
	rf.vote()
}

func (rf *Raft) leaderHb() {
	rf.role = 3
	rf.appendEmpty()
}

func (rf *Raft) transitionToFollower() {
	if rf.ticker != nil {
		rf.ticker.Stop()
	}
	rf.role = 1
	rf.timeout(rf.followerHb)
}

func (rf *Raft) calElectionTimeout() int64 {
	n := rand.Int63n(rf.electionTimeout.Milliseconds()) + rf.electionTimeout.Milliseconds()
	return n
}

func (rf *Raft) logMatch([]LogEntry, int, int) bool {
	return true
}
