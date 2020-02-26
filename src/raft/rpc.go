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
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(j, &args, &reply)
			//DPrintf("[%d] sent vote to [%d] reply-[Term:%d][VoteGranted:%v]\n", rf.me, j, reply.Term, reply.VoteGranted)
			voteCount += 1

			if ok && reply.VoteGranted {
				quorum += 1
			} else if reply.Term > rf.currentTerm {
				rf.lock.Lock()
				rf.role = 1
				rf.updateTime = time.Now()
				DPrintf("[%v]-[%d] update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.lock.Unlock()
			}

			if quorum >= (len(rf.peers)/2 + 1) {
				rf.lock.Lock()
				if rf.role == 3 {
					rf.lock.Unlock()
					return
				}
				DPrintf("[%d] election #win transition to leader [term:%d]\n", rf.me, rf.currentTerm)
				rf.leaderId = rf.me
				rf.role = 3
				rf.lock.Unlock()
				rf.leaderHb()
				go rf.heartbeat(rf.leaderHb)

			} else if voteCount == len(peers)-1 {
				//election complete voteCount be equals to peers count -1
				DPrintf("[%d] election #lose transition to follower [term:%d]\n", rf.me, rf.currentTerm)
				go rf.transitionToFollower()
			}
		}(i)
	}
}

func (rf *Raft) appendLog(command interface{}) (prev LogEntry, entry LogEntry) {
	prev = rf.log[len(rf.log)-1]
	entry = LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log),
		Command: command,
	}
	rf.log = append(rf.log, entry)

	DPrintf("[%d]-[log:%+v]\n", rf.me, rf.log)
	return prev, entry
}

func (rf *Raft) appendEmpty() {
	rf.lock.Lock()
	request := RequestAppendEntries{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.lock.Unlock()
	reply := ReplyAppendEntries{}
	rf.append(request, reply)
}

func (rf *Raft) append(request RequestAppendEntries, reply ReplyAppendEntries) {

	agree := 0

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(j int) {
			ok := rf.sendAppendEntries(j, &request, &reply)
			if ok && reply.Success {
				agree += 1
			}

			if len(request.Entries) > 0 && agree == len(rf.peers)-1 {
				entry := request.Entries[0]
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: entry.Index,
					Command:      entry.Command,
				}
				DPrintf("apply\n")
				rf.applyCh <- msg
			}

			if reply.Term > rf.currentTerm {
				rf.lock.Lock()
				rf.role = 1
				rf.updateTime = time.Now()
				DPrintf("[%v]-[%d] update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term

				rf.lock.Unlock()
				go rf.transitionToFollower()
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("[%d] voting for [%d] [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)

	//two nodes vote at the same time may cause this raft node vote for two member ,so we have to lock
	rf.lock.Lock()
	defer rf.lock.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf("[%d] grant [%d]  [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.updateTime = time.Now()
		DPrintf("[%v]-[%d] update term from [%d] to [%d]\n", rf.updateTime, rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	} else if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		DPrintf("[%d] grant [%d] [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d] reject [%d] [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	rf.lock.Lock()
	defer rf.lock.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

	} else if args.Term >= rf.currentTerm &&
		(len(args.Entries) == 0 || len(args.Entries) > 0 && rf.logMatch(args.Entries, args.PrevLogTerm, args.PrevLogIndex)) {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		if rf.leaderId != args.LeaderId {
			rf.leaderId = args.LeaderId
		}
		rf.updateTime = time.Now()
		DPrintf("[%v]-[%d] update term from [%d] to [%d]; leader from [%d] to [%d]\n",
			rf.updateTime, rf.me, rf.currentTerm, args.Term, rf.leaderId, args.LeaderId)
		if len(args.Entries) > 0 {
			DPrintf("[%d] accept [%d]'s append-[%+v]\n", rf.me, args.LeaderId, args)
			entry := args.Entries[0]
			rf.appendLog(entry.Command)
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: entry.Index,
				Command:      entry.Command,
			}
			rf.applyCh <- msg
		}
		go rf.transitionToFollower()
		reply.Term = rf.currentTerm
		reply.Success = true

	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

}
