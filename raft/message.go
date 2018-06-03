package raft

type LogEntry struct {
	Index   int
	Term    int         // when entry was received by leader
	Command interface{} // command for state machine
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}
