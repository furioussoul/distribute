package raft

import (
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

	DPrintf("[%d] election start[role:%d][term:%d][votefor:%d]\n", rf.me, rf.role, rf.currentTerm, rf.votedFor)
	peers := rf.peers
	quorum := 1
	voteCount := 0

	for i := range peers {
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

			rf.lock.Lock()
			defer rf.lock.Unlock()

			//DPrintf("[%d] sent vote to [%d] reply-[Term:%d][VoteGranted:%v]\n", rf.me, j, reply.Term, reply.VoteGranted)
			voteCount += 1

			if ok && reply.VoteGranted {
				quorum += 1
			} else if reply.Term > rf.currentTerm {
				rf.role = 1
				rf.updateTime = time.Now()
				DPrintf("[%v]-[%d] vote update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.transitionToFollower()
			}

			if quorum == len(rf.peers)/2+1 {
				if rf.role != 3 {
					rf.transitionToLeader()
				}

			} else if voteCount == len(peers)-1 && rf.role != 3 {
				//election complete voteCount be equals to peers count -1
				DPrintf("[%d] election #lose [term:%d]\n", rf.me, rf.currentTerm)
				rf.votedFor = -1
				rf.leaderId = -1
				go rf.timeout(func() {
					rf.lock.Lock()
					rf.currentTerm++
					rf.votedFor = rf.me
					rf.lock.Unlock()
					rf.vote()
				})
			}

			rf.persist()
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

		rf.lock.Lock()
		defer rf.lock.Unlock()

		if !ok {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.role = 1
			rf.updateTime = time.Now()
			DPrintf("[%v]-[%d] appendEmpty update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term
			rf.transitionToFollower()
		}
		rf.persist()
	}(i)
}

func (rf *Raft) appendToMembers(i int) {

	go func(j int) {
		rf.lock.Lock()
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

		rf.lock.Unlock()

		ok := rf.sendAppendEntries(j, &request, &reply)

		rf.lock.Lock()
		defer rf.lock.Unlock()

		if !ok {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.role = 1
			rf.updateTime = time.Now()
			DPrintf("[%v]-[%d] appendToMembers update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term
			rf.transitionToFollower()
		} else {
			if reply.Success {
				rf.matchIndex[j] = logIndex
				rf.nextIndex[j]++
			} else {
				rf.nextIndex[j]--
			}
		}
	}(i)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("[%d] voting for [%d] [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)

	//two nodes vote at the same time may cause this raft node vote for two member ,so we have to lock
	rf.lock.Lock()
	defer rf.lock.Unlock()

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d] reject [%d] [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logNewer(args) {

		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term
		rf.transitionToFollower()
	}

	rf.persist()
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	rf.lock.Lock()
	defer rf.lock.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return

	}
	if len(args.Entries) == 0 || rf.logMatch(args.PrevLogTerm, args.PrevLogIndex) {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
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
		rf.currentTerm = args.Term
		rf.transitionToFollower()
	}

	rf.persist()
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
