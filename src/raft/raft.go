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
	"errors"
	"fmt"
	"math/rand"
	"sort"
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
	lock         sync.Mutex
	persistLock  sync.Mutex
	appenderLock sync.Mutex
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()

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

	memberAppending []int32
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
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
	defer rf.lock.Unlock()

	index := -1
	term := -1
	isLeader := rf.me == rf.leaderId

	// Your code here (2B).
	if isLeader {

		entry := LogEntry{
			Term:    rf.currentTerm,
			Index:   len(rf.log),
			Command: command,
		}

		DPrintf("leader [%d][term:%d] accept log [%+v]", rf.me, rf.currentTerm, entry)

		index, term = rf.appendLogToLocal(entry)
	}

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
	// Your code here, if desired.
	DPrintf("[%d] crash term:[%d] voteFor:[%d] log:[%+v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
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
	rf.heartBeatInterval = 40 * time.Millisecond
	rf.electionTimeout = 200 * time.Millisecond
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.memberAppending = make([]int32, len(peers))
	for i := range peers {
		rf.memberAppending[i] = 0
	}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{0, 0, nil}}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.transitionToFollower()

	DPrintf("[%d] start term:[%d] voteFor:[%d] log:[%+v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)

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

func (rf *Raft) leaderHb() {
	rf.appendToMembers()
	rf.resetCommitIndex()
}

//只commit当前term的日志,figure8描述了之前term的on major的log也会被覆盖
func (rf *Raft) resetCommitIndex() {

	ids := make([]int, 0)

	ids = append(ids, len(rf.log)-1)
	for i := range rf.peers {
		if rf.me != i {
			ids = append(ids, rf.matchIndex[i])
		}
	}

	sort.Ints(ids)

	agreeIndex := 0
	agree := 0

	for i := len(ids) - 1; i >= 0; i-- {

		tmp := ids[i]

		for j := len(ids) - 1; j >= 0; j-- {
			if tmp <= ids[j] {
				//DPrintf("[%d] [%d] [%d] [%+v]", tmp,ids[j],j,ids)
				agree++
			}
		}

		if agree >= len(rf.peers)/2+1 {
			agreeIndex = tmp
			break
		} else {
			agree = 0
		}
	}

	//DPrintf("[%d],agreeIndex:[%d],commitIndex:[%d]", rf.me, agreeIndex, rf.commitIndex)
	if rf.log[agreeIndex].Term == rf.currentTerm && agreeIndex > rf.commitIndex {
		DPrintf("[%d] commit index:[%d] matchIndex:[%+v]\n", rf.me, agreeIndex, rf.matchIndex)

		if agreeIndex > rf.commitIndex {
			for i := rf.commitIndex; i <= agreeIndex; i++ {
				entry := rf.log[i]
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: entry.Index,
					Command:      entry.Command,
				}
				rf.applyCh <- msg
			}
		}

		rf.commitIndex = agreeIndex
	}
}

func (rf *Raft) transitionToLeader() {

	if rf.role == 3 {
		return
	}

	if rf.ticker != nil {
		rf.ticker.Stop()
	}

	DPrintf("[%d] election #win transition to leader [term:%d]\n", rf.me, rf.currentTerm)

	rf.role = 3
	rf.leaderId = rf.me
	l := len(rf.nextIndex)
	for i := 0; i < l; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.heartbeat(rf.leaderHb)
}

func (rf *Raft) transitionToCandidate() {

	if rf.ticker != nil {
		rf.ticker.Stop()
	}

	rf.role = 2
	DPrintf("[%v] transitionToCandidate update term from [%d] to [%d]\n", rf.me, rf.currentTerm, rf.currentTerm+1)

	go rf.timeout(rf.vote)
}

func (rf *Raft) transitionToFollower() {

	if rf.ticker != nil {
		rf.ticker.Stop()
	}

	rf.role = 1
	DPrintf("[%d] transition to follower [term:%d]\n", rf.me, rf.currentTerm)

	go rf.timeout(rf.transitionToCandidate)
}

func (rf *Raft) calElectionTimeout() int64 {
	n := rand.Int63n(rf.electionTimeout.Milliseconds()) + rf.electionTimeout.Milliseconds()
	return n
}

func (rf *Raft) logMatch(prevTerm int, prevIndex int) bool {

	flag := true

	if prevIndex > len(rf.log)-1 {
		flag = false
	} else {
		prev := rf.log[prevIndex]
		if prev.Term != prevTerm {
			flag = false
		}
	}

	if !flag {
		//DPrintf("[%d] log mismatch, prevIndex:[%d],prevTerm:[%d],requestEntry:[%+v],myLog:[%+v]", rf.me, prevIndex, prevTerm, logEntries[0], rf.log)
	}
	return flag
}

func (rf *Raft) setTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.leaderId = -1
		rf.votedFor = -1
		rf.persist()
		DPrintf("[%d] Set term [%d]", rf.me, term)
	}
}

func (rf *Raft) setLastVoteFor(candidate int) error {

	rf.lock.Lock()
	defer rf.lock.Unlock()

	if rf.votedFor != -1 && candidate != -1 {
		errMsg := fmt.Sprintf("[%d] Already voted for another candidate", rf.me)
		DPrintf(errMsg)
		return errors.New(errMsg)
	}

	rf.votedFor = candidate
	rf.persist()

	if candidate != -1 {
		DPrintf("[%d] vote for [%d]", rf.me, candidate)
	} else {
		DPrintf("[%d] reset lastVoteFor = -1", rf.me)
	}

	return nil
}

func (rf *Raft) appendLogToLocal(entry LogEntry) (index int, term int) {

	rf.appenderLock.Lock()
	defer rf.appenderLock.Unlock()

	if entry.Index <= len(rf.log)-1 {
		if rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		} else {
			DPrintf("[%d] ignore logEntry already in the log", rf.me)
			return
		}
	}

	rf.log = append(rf.log, entry)
	DPrintf("[%d]-appendLogToLocal-[%+v]\n", rf.me, entry)
	index = entry.Index
	term = entry.Term

	rf.persist()

	return
}
