package raft

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//two nodes vote at the same time may cause this raft node vote for two member ,so we have to lock
	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.killed() {
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[%d] grant [%d]'s vote [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

	} else if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		DPrintf("[%d] grant [%d]'s vote [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

	} else {
		reply.VoteGranted = false
		DPrintf("[%d] reject [%d]'s vote [currentTerm:%d][requestTerm:%d]\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}
}

func (rf *Raft) AppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {

	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.killed() {
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.role = 1
	if args.Term > rf.currentTerm {
		DPrintf("[id:%d] update term from [%d] to [%d]; leader from [%d] to [%d]\n",
			rf.me, rf.currentTerm, args.Term, rf.leaderId, args.LeaderId)
		rf.currentTerm = args.Term
	}
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
	}
	go rf.transitionToFollower()
	reply.Term = rf.currentTerm
	reply.Success = true

}
