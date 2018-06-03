package raft

type LogEntry struct {
	Index   int
	Term    int         // when entry was received by leader
	Command interface{} // command for state machine
}
