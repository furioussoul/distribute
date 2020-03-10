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
	Term          int
	ConflictIndex int
	ConflictTerm  int
	Success       bool
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
	case <-time.After(250 * time.Millisecond):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {

	ch := make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(250 * time.Millisecond):
		return false
	}
}

func (rf *Raft) vote() {

	DPrintf("[%d] start vote term[%d]", rf.me, rf.currentTerm)

	rf.setTerm(rf.currentTerm + 1)
	if err := rf.setLastVoteFor(rf.me); err != nil {
		return
	}

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

	rf.voteTimeoutTicker = time.NewTicker(time.Duration(1000) * time.Millisecond)

	go func() {
		select {
		case <-rf.voteTimeoutTicker.C:
			if !atomic.CompareAndSwapInt32(&cancelQuorum, 0, 1) {
				return
			}
			DPrintf("[%d] vote timeout restart vote", rf.me)
			rf.vote()
		}
	}()

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

			if !ok || rf.role != 2 || args.Term != rf.currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
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

func (rf *Raft) appendToMembers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] == len(rf.log)-1 {
			rf.appendEmpty(i)
		} else {
			rf.appendLogEntry(i)
		}
	}
}

func (rf *Raft) appendEmpty(i int) {

	go func(j int) {

		reply := ReplyAppendEntries{}
		request := RequestAppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		DPrintf("[%d] send AppendEmpty to [%d]", rf.me, j)

		ok := rf.sendAppendEntries(j, &request, &reply)

		if !ok || request.Term != rf.currentTerm || rf.role != 3 {
			return
		}

		if reply.Term > rf.currentTerm {
			//DPrintf("[%v]-[%d] appendEmpty update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
			rf.setTerm(reply.Term)
			rf.transitionToFollower()
		}

	}(i)
}

func (rf *Raft) appendLogEntry(i int) {

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

		if !ok || request.Term != rf.currentTerm || rf.role != 3 {
			rf.memberAppending[j] = 0
			return
		}

		DPrintf("[%d] send AppendEntries to [%d] \nrequest:[%+v]\nresponse:[%+v]", rf.me, j, request, reply)

		if reply.Term > rf.currentTerm {

			DPrintf("[%d] appendToMembers update term from [%d] to [%d]\n", rf.me, rf.currentTerm, reply.Term)
			rf.setTerm(reply.Term)
			rf.transitionToFollower()

		} else if reply.Success {

			rf.matchIndex[j] = logIndex
			rf.nextIndex[j]++

		} else {

			//DPrintf("[%d]-[%+v]", rf.me, rf.log)

			if reply.ConflictTerm == -1 {

				rf.nextIndex[j] = reply.ConflictIndex

			} else {

				found := false
				nextIndex := 0

				for i := len(rf.log) - 1; i > 0; i-- {
					if reply.ConflictTerm == rf.log[i].Term {
						found = true
						nextIndex = i
						break
					}
				}

				if found {
					rf.nextIndex[j] = nextIndex + 1
				} else {
					rf.nextIndex[j] = reply.ConflictIndex
				}
			}
		}

	}(i)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	DPrintf("[%d] accept RequestVote from [%d] , args:[%+v]", rf.me, args.CandidateId, args)

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	if args.Term > rf.currentTerm {

		rf.setTerm(args.Term)
		rf.transitionToFollower()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logNewer(args) {

		if err := rf.setLastVoteFor(args.CandidateId); err != nil {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	}
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {

		reply.Success = false

		return
	}

	/*if rf.currentTerm != args.Term {
		DPrintf("[%d] AppendEntries update term from [%d] to [%d]; leader from [%d] to [%d]\n",
			rf.me, rf.currentTerm, args.Term, rf.leaderId, args.LeaderId)
	}*/

	if args.Term > rf.currentTerm {
		rf.setTerm(args.Term)
	}

	if len(args.Entries) == 0 {

		if rf.leaderId != args.LeaderId {
			rf.leaderId = args.LeaderId
		}

		reply.Success = true

		rf.commit(args)
		rf.transitionToFollower()

		return

	}

	if args.PrevLogIndex > len(rf.log)-1 {

		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1

	} else {

		prevTerm := rf.log[args.PrevLogIndex].Term

		if prevTerm != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = prevTerm
			for i, e := range rf.log {
				if e.Term == prevTerm {
					reply.ConflictIndex = i
					break
				}
			}

			//DPrintf("[%d]-[%+v]", rf.me, rf.log)

		} else {
			reply.Success = true
			rf.appendLogToLocal(args.Entries[0])
			rf.commit(args)
		}
	}

	rf.transitionToFollower()
}

func (rf *Raft) commit(args *RequestAppendEntries) {

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
