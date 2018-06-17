package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/girikuncoro/go-raft/labrpc"
)

// import "bytes"
// import "encoding/gob"

type NodeState string

const (
	StateFollower  NodeState = "StateFollower"
	StateCandidate           = "StateCandidate"
	StateLeader              = "StateLeader"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// General state
	nodeID string
	me     int // this peer's index into peers[]
	state  NodeState

	// Election state
	currentTerm int
	votedFor    string // leaderID that is voted for in current term, empty string of no vote
	leaderID    string

	// Log state
	log         []LogEntry
	commitIndex int // index of highest log entry known to be committed (init 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (init 0, increases monotonically)

	// Leader state
	nextIndex  []int // for each server, index of next log entry
	matchIndex []int // for each server, index of highest log entry known to be replicated

	// Heartbeat
	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateFollower
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteChan chan int) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		voteChan <- server
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()

	return rf
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration {
		// Randomized timeouts between [300,500]-ms
		return (300 + time.Duration(rand.Intn(200))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := time.Now().Add(currentTimeout)

	if rf.state != StateLeader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
		// Start election process if not leader and no heartbeat within the random election timeout
		go rf.beginElection()
	}
	go rf.startElectionProcess()
}

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	rf.becomeCandidate()
	log.Println("Begin election")

	// Request vote from peers
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.nodeID,
		LastLogTerm:  rf.currentTerm,
		LastLogIndex: rf.commitIndex,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &req, &replies[i], voteChan)
		}
	}
	rf.mu.Unlock()

	votes := 1
	for i := 0; i < len(replies); i++ {
		reply := replies[<-voteChan]

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			log.Println("Step down as leader/candidate, become follower")
			rf.becomeFollower(reply.Term)
			break
		} else if votes += reply.VoteCounted(); votes > len(replies)/2 {
			// Promote to leader if has majority vote
			if rf.state == StateCandidate {
				log.Println("Election won, become leader")
				rf.becomeLeader()
			}
		}
		rf.mu.Unlock()
	}
}

// Become candidate, increase term and vote for itself
func (rf *Raft) becomeCandidate() {
	rf.state = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.nodeID
}

// Become follower, reassign term with reply term
func (rf *Raft) becomeFollower(term int) {
	rf.state = StateFollower
	rf.currentTerm = term
	rf.votedFor = ""
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
}
