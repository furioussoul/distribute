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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"
)

import "sync/atomic"
import "../labrpc"
import "../labgob"

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
	SnapShot     []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	role      Role                //1 follower; 2 candidate; 3 leader;
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	electionTimeout   time.Duration
	heartBeatInterval time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int

	lastIncludedIndex int
	lastIncludedTerm  int

	nextIndex  []int
	matchIndex []int

	applyCh     chan ApplyMsg
	killCh      chan bool
	voteCh      chan bool
	appendLogCh chan bool
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	NULL int = -1
)

type Role int

const (
	Follower  Role = iota
	Candidate Role = iota
	Leader    Role = iota
)

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

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type ReplyAppendEntries struct {
	Term          int
	ConflictIndex int
	ConflictTerm  int
	Success       bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == Leader
	index := NULL

	if isLeader {
		index = rf.logLen()
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}

		DPrintf("leader [%d][term:%d] accept log [%+v]\n", rf.me, term, entry)

		rf.log = append(rf.log, entry)
		rf.persist()
		rf.leaderHb()
	}

	return index, term, isLeader
}

func sendToCh(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
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
	sendToCh(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) DoSnapshot(applyIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if applyIndex <= rf.lastIncludedIndex {
		return
	}
	//update last included index & term
	rf.log = append(make([]LogEntry, 0), rf.log[applyIndex-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = applyIndex
	rf.lastIncludedTerm = rf.getLog(applyIndex).Term
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) getLog(i int) LogEntry {
	return rf.log[i-rf.lastIncludedIndex]
}

func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(prevLogIdx).Term
}

func (rf *Raft) getLastLogIdx() int {
	return rf.logLen() - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(idx).Term
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
	rf.role = Follower
	rf.me = me

	rf.heartBeatInterval = 100 * time.Millisecond
	rf.electionTimeout = 250 * time.Millisecond

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.killCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)

	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = []LogEntry{{0, 0, nil}}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.killCh:
				return
			default:
				//
			}

			rf.mu.Lock()
			role := rf.role
			rf.mu.Unlock()

			switch role {
			case Follower, Candidate:
				select {
				case <-rf.voteCh:
				case <-rf.appendLogCh:
				case <-time.After(time.Duration(rf.calElectionTimeout()) * time.Millisecond):
					rf.mu.Lock()
					rf.transitionToCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.leaderHb()
				time.Sleep(rf.heartBeatInterval)
			}
		}
	}()

	return rf
}

func (rf *Raft) sendSnapshot(server int) {
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LastIncludeIndex: rf.lastIncludedIndex,
		LastIncludeTerm:  rf.lastIncludedTerm,
		LeaderId:         rf.me,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ret := rf.sendInstallSnapshot(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ret || rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm { //all server rule 1 If RPC response contains term T > currentTerm:
		rf.transitionToFollower(reply.Term) // set currentTerm = T, convert to follower (§5.1)
		return
	}
	rf.updateNextMatchIdx(server, rf.lastIncludedIndex)
}

func (rf *Raft) updateNextMatchIdx(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

func (rf *Raft) leaderHb() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(j int) {
			for {

				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[j]-rf.lastIncludedIndex < 1 {
					//log compact already done
					//so i just sendInstall RPC
					rf.sendSnapshot(j)
					return
				}

				DPrintf("[%d] --  nextIndex [%d] -- matchIndex [%d] -- lastIncludedIndex [%d]",
					j, rf.nextIndex[j], rf.matchIndex[j], rf.lastIncludedIndex)
				args := RequestAppendEntries{
					rf.currentTerm,
					rf.me,
					rf.getPrevLogIdx(j),
					rf.getPrevLogTerm(j),
					//If last log index ≥ nextIndex for a follower:send AppendEntries RPC with log entries starting at nextIndex
					//nextIndex > last log index, rf.log[rf.nextIndex[idx]:] will be empty then like a heartbeat
					append(make([]LogEntry, 0), rf.log[rf.nextIndex[j]-rf.lastIncludedIndex:]...),
					rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &ReplyAppendEntries{}
				ok := rf.sendAppendEntries(j, &args, reply)
				//DPrintf("[%d] args -- [%+v] \n[%d] reply [%+v]", j, args, j, reply)
				if !ok {
					return
				}

				rf.mu.Lock()

				if rf.role != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm { //all server rule 1 If RPC response contains term T > currentTerm:
					rf.transitionToFollower(reply.Term) // set currentTerm = T, convert to follower (§5.1)
					rf.mu.Unlock()
					return
				}

				if reply.Success { //If successful：update nextIndex and matchIndex for follower
					rf.updateNextMatchIdx(j, args.PrevLogIndex+len(args.Entries))
					rf.mu.Unlock()
					return
				} else {
					tarIndex := reply.ConflictIndex
					//If it does not find an entry with that term
					if reply.ConflictTerm != NULL {
						logSize := rf.logLen()
						//first search its log for conflictTerm
						for i := rf.lastIncludedIndex; i < logSize; i++ {
							if rf.getLog(i).Term != reply.ConflictTerm {
								continue
							}
							//找最后一个
							//if it finds an entry in its log with that term,
							for i < logSize && rf.getLog(i).Term == reply.ConflictTerm {
								i++
							} //set nextIndex to be the one
							tarIndex = i //beyond the index of the last entry in that term in its log
						}
					}
					rf.nextIndex[j] = min(rf.logLen(), tarIndex)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.getLastLogIdx()
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	//将所有节点的matchIndex倒序排,中间节的就是commitIndex
	N := copyMatchIndex[len(copyMatchIndex)/2]
	//只commit当前term的日志
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

func (rf *Raft) transitionToLeader() {

	if rf.role == Leader {
		return
	}

	DPrintf("[%d] election #win transition to leader [term:%d]\n", rf.me, rf.currentTerm)

	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	l := len(rf.nextIndex)
	for i := 0; i < l; i++ {
		rf.nextIndex[i] = rf.logLen()
		rf.matchIndex[i] = 0
	}
	rf.leaderHb()
}

func (rf *Raft) transitionToCandidate() {
	if rf.role != Candidate {
		DPrintf("[%d] transitionToCandidate -- term -- [%d]\n", rf.me, rf.currentTerm)
	}

	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	go rf.startElection()
}

func (rf *Raft) transitionToFollower(term int) {
	if rf.role != Follower {
		DPrintf("[%d] transitionToCandidate -- term -- [%d]\n", rf.me, rf.currentTerm)
	}
	rf.role = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) calElectionTimeout() int64 {
	n := rand.Int63n(rf.electionTimeout.Milliseconds()) + rf.electionTimeout.Milliseconds()
	return n
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {

	quorum := makeQuorum(len(rf.peers)/2+1, func(elected bool) {
		if elected {
			rf.transitionToLeader()
			sendToCh(rf.voteCh)
		}
	})

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTerm(),
		LastLogIndex: rf.getLastLogIdx(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(j int) {

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(j, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.transitionToFollower(reply.Term)
				return
			}

			if rf.role != Candidate || args.Term != rf.currentTerm {
				return
			}

			if reply.VoteGranted {
				quorum.succeed()
			} else {
				quorum.fail()
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		//false
	} else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {
		//false
	} else if args.LastLogTerm < rf.getLastLogTerm() {
		//false
	} else if args.LastLogTerm == rf.getLastLogTerm() &&
		args.LastLogIndex < rf.getLastLogIdx() {
		//false
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.role = Follower
		rf.persist()
		sendToCh(rf.voteCh)
	}
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}

	defer sendToCh(rf.appendLogCh)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = NULL
	reply.ConflictIndex = 0

	prevLogIndexTerm := NULL

	if args.PrevLogIndex < rf.lastIncludedIndex {
		//already installed snapshot, let nextIndex= follower's last log index + 1
		reply.ConflictIndex = rf.logLen()
		return
	}

	if args.PrevLogIndex < rf.logLen() {
		prevLogIndexTerm = rf.getLog(args.PrevLogIndex).Term
	}

	//PrevLogTerm冲突
	if args.PrevLogTerm != prevLogIndexTerm {
		reply.ConflictIndex = rf.logLen()
		if prevLogIndexTerm != NULL {
			reply.ConflictTerm = prevLogIndexTerm
			for i := rf.lastIncludedIndex; i < rf.logLen(); i++ {
				if rf.getLog(i).Term == reply.ConflictTerm {
					//找第一个
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	//没有冲突
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index < rf.logLen() {
			if rf.getLog(index).Term == args.Entries[i].Term {
				continue
			} else { //3. If an existing entry conflicts with a new one (same index but different terms),
				rf.log = rf.log[:index-rf.lastIncludedIndex] //delete the existing entry and all that follow it (§5.3)
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...) //4. Append any new entries not already in the log
		rf.persist()
		break
	}
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIdx())
		rf.updateLastApplied()
	}
	reply.Success = true
}

func (rf *Raft) updateLastApplied() {
	rf.lastApplied = max(rf.lastIncludedIndex, rf.lastApplied)
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{
			true,
			curLog.Command,
			rf.lastApplied,
			nil,
		}
		rf.applyCh <- applyMsg
		if rf.role == Leader {
			DPrintf("[%d] commit [%d] ", rf.me, rf.lastApplied)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	defer sendToCh(rf.appendLogCh)

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}

	if args.LastIncludeIndex <= rf.lastIncludedIndex {
		//ignore old install rpc
		return
	}

	applyMsg := ApplyMsg{CommandValid: false, SnapShot: args.Data}
	if args.LastIncludeIndex < rf.getLastLogIdx() {
		// [ rf.lastIncludedIndex _ _ _ args.LastIncludeIndex _ _ _ lastLogIdx ]
		rf.log = append(make([]LogEntry, 0), rf.log[args.LastIncludeIndex-rf.lastIncludedIndex:]...)
	} else {
		//7. Discard the entire log
		rf.log = []LogEntry{{args.LastIncludeTerm, args.LastIncludeIndex, nil}}
	}

	//Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludeIndex, args.LastIncludeTerm
	rf.persistWithSnapShot(args.Data)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	} //snapshot is older than kvserver's db, so reply immediately
	rf.applyCh <- applyMsg
}

func (rf *Raft) persistWithSnapShot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&log) == nil &&
		d.Decode(&lastIncludedIndex) == nil &&
		d.Decode(&lastIncludedTerm) == nil {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
		rf.mu.Unlock()
	} else {
		DPrintf("readPersist ERROR for server %v\n", rf.me)
	}
}
