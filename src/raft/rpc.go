package raft

import (
	"sync/atomic"
	"time"
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
	Term    int
	Success bool
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

	ch := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) vote() {

	DPrintf("[%d] start vote term[%d]", rf.me, rf.currentTerm)

	rf.setTerm(rf.currentTerm + 1)
	rf.setLastVoteFor(rf.me)

	var cancelQuorum int32

	quorum := makeQuorum(len(rf.peers)/2+1, func(elected bool) {

		if !atomic.CompareAndSwapInt32(&cancelQuorum, 0, 1) {
			return
		}

		if elected {
			rf.transitionToLeader()
		} else {
			rf.transitionToFollower()
		}
	})

	go rf.timeout(func() {
		if !atomic.CompareAndSwapInt32(&cancelQuorum, 0, 1) {
			return
		}
		DPrintf("[%d] vote timeout restart vote", rf.me)
		rf.vote()
	})

	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(j int) {

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
				LastLogIndex: len(rf.log) - 1,
			}
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(j, &args, &reply)

			if !ok {
				DPrintf("vote err")
				quorum.fail()
			} else if reply.Term > rf.currentTerm {
				DPrintf("Received greater term from [%d]", j)
				atomic.CompareAndSwapInt32(&cancelQuorum, 0, 1)
				rf.setTerm(reply.Term)
				rf.transitionToFollower()
			} else if !reply.VoteGranted {
				DPrintf("[%d] vote fail1 from [%d]", rf.me, j)
				quorum.fail()
			} else if rf.currentTerm != reply.Term {
				DPrintf("[%d] vote fail2 from [%d]", rf.me, j)
				quorum.fail()
			} else {
				DPrintf("[%d] vote succeed from [%d]", rf.me, j)
				quorum.succeed()
			}
		}(i)
	}
}

func (rf *Raft) appendLogToLocal(entry LogEntry) (index int, term int) {
	if entry.Index <= len(rf.log)-1 {
		if rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		} else {
			DPrintf("[%d] ignore logEntry already in the log", rf.me)
			return
		}
	}

	rf.log = append(rf.log, entry)
	DPrintf("[%d]-appendLogToLocal-[%+v]\n[%+v]\n", rf.me, entry, rf.log)
	index = entry.Index
	term = entry.Term

	rf.persist()
	return
}

func (rf *Raft) appendEmpty(i int) {

	go func(j int) {
		reply := ReplyAppendEntries{}
		request := RequestAppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		ok := rf.sendAppendEntries(j, &request, &reply)

		if !ok {
			return
		}

		if reply.Term > rf.currentTerm {
			DPrintf("[%v]-[%d] appendEmpty update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
			rf.setTerm(reply.Term)
			rf.transitionToFollower()
		}
	}(i)
}

func (rf *Raft) appendToMembers(i int) {

	go func(j int) {
		prevIndex := rf.nextIndex[j] - 1
		logIndex := rf.nextIndex[j]

		if logIndex >= len(rf.log) {
			logIndex--
			prevIndex--
		}

		prev := rf.log[prevIndex]
		entry := rf.log[logIndex]

		reply := ReplyAppendEntries{}
		request := RequestAppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      []LogEntry{entry},
			PrevLogIndex: prev.Index,
			PrevLogTerm:  prev.Term,
			LeaderCommit: rf.commitIndex,
		}

		ok := rf.sendAppendEntries(j, &request, &reply)

		if !ok {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.updateTime = time.Now()
			DPrintf("[%v]-[%d] appendToMembers update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
			rf.setTerm(reply.Term)
			rf.transitionToFollower()
			return
		}

		if reply.Success {
			rf.matchIndex[j] = logIndex
			rf.nextIndex[j]++
		} else {
			rf.nextIndex[j]--
		}

	}(i)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	DPrintf("[%d] accept RequestVote from [%d] , args:[%+v]", rf.me, args.CandidateId, args)

	if args.Term > rf.currentTerm {

		rf.setTerm(args.Term)
		rf.transitionToFollower()

	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logNewer(args) {

		rf.setLastVoteFor(args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	}
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return

	}
	if len(args.Entries) == 0 || rf.logMatch(args.PrevLogTerm, args.PrevLogIndex) {
		if args.Term > rf.currentTerm {
			rf.setTerm(args.Term)
		}
		if rf.leaderId != args.LeaderId {
			rf.leaderId = args.LeaderId
		}
		rf.updateTime = time.Now()
		if rf.currentTerm != args.Term {
			DPrintf("[%v]-[%d] AppendEntries update term from [%d] to [%d]; leader from [%d] to [%d]\n",
				rf.updateTime, rf.me, rf.currentTerm, args.Term, rf.leaderId, args.LeaderId)
		}

		if len(args.Entries) > 0 {
			DPrintf("[%d] prevIndex:[%d] prevTerm:[%d]", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			rf.appendLogToLocal(args.Entries[0])
		}

		//DPrintf("[%d]-[%+v]-[%+v]", rf.me, args, rf.log)

		rf.transitionToFollower()
		reply.Term = rf.currentTerm
		reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			prevCommitIndex := rf.commitIndex
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			if rf.commitIndex > prevCommitIndex {
				for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.log[i].Index,
						Command:      rf.log[i].Command,
					}
					DPrintf("[%d] commit index:[%d]\n", rf.me, i)
					rf.applyCh <- msg
				}
			}
		}

	} else {
		reply.Success = false
		rf.transitionToFollower()
	}

	if args.Term > rf.currentTerm {
		rf.setTerm(args.Term)
		rf.transitionToFollower()
	}
}

func (rf *Raft) logNewer(args *RequestVoteArgs) bool {

	flag := false

	if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		flag = true
	} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 {
		flag = true
	} else {
		flag = false
	}

	return flag
}
