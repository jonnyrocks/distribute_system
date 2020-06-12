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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
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

// hold log entries
type LogEntry struct {
	Command interface{}
	Term    int
}

type State string

// enum
const (
	Follower  State = "Follower"
	Candidate       = "Candidate"
	Leader          = "Leader"
)

const appendEntriesTimout = time.Duration(150 * time.Millisecond)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    *int
	log         []LogEntry
	voteCount   int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	timer   *time.Timer
	timeout time.Duration

	appendCh chan bool
	voteCh   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RequestVote RPC call on [%v], received term: %v, currentTerm %v", rf.me, args.Term, rf.currentTerm)

	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}
	reply.Term = rf.currentTerm
	DPrintf("[%v] received vote request to vote for [%v]", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && args.LastLogIndex >= len(rf.log)-1 {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: reset timer
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		// Candidate: discover current leader or new term
		// Leader: discover leader with higher term
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	go func() {
		DPrintf("[%d] received AE, args.Term: %v, currentTerm: %v", rf.me, args.Term, rf.currentTerm)
		rf.appendCh <- true
	}()

	if len(rf.log) == 0 || len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("PrevLogIndex term != log")
		// DPrintf("rf.log=[%v], args.PrevLogIndex=[%v], args.PrevLogTerm=[%v]", rf.log, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		return
	}

	// TODO:  If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it

	// Append any new entries not already in the log

	// If leaderCommit > commitIndex, set commitIndex min(leaderCommit, index of last new entry)

	// execute append entries

	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = &rf.me
	lastLogTerm := -1
	term := rf.currentTerm
	rf.voteCount = 1
	rf.timer.Reset(rf.timeout)
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	DPrintf("++++Initial election from node:%v for term %v+++", rf.me, term)
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  lastLogTerm}
		reply := RequestVoteReply{}

		go func(server int) {
			voteGranted := rf.sendRequestVote(server, &args, &reply)
			if !voteGranted {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchStateTo(Follower)
				}
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			rf.voteCount++
			rf.mu.Unlock()
			DPrintf("[%d] got vote from %d", rf.me, server)
		}(server)
	}
	// DPrintf("leaderElection complete for %v", rf.me)
}

func (rf *Raft) logReplication() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// prevLogIndex: len(rf.log),
			// prevLogTerm:
			// lastLogTerm:  rf.log[len(rf.log)-1].term
		}
		reply := AppendEntriesReply{}
		DPrintf("sendAppendzEntries from [%v] to [%v]", rf.me, server)
		rf.mu.Unlock()

		go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchStateTo(Follower)
				}
				rf.mu.Unlock()
			}
		}(server, args, reply)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) switchStateTo(state State) {
	// TODO: ss
	if rf.state == state {
		return
	}
	if state == Follower {
		rf.state = Follower
		rf.votedFor = nil
		DPrintf("[%v] become follower in term:%v\n", rf.me, rf.currentTerm)
	}
	if state == Candidate {
		rf.state = Candidate
		DPrintf("[%v] become candidate in term:%v\n", rf.me, rf.currentTerm)
	}
	if state == Leader {
		rf.state = Leader
		DPrintf("[%v] become leader in term:%v\n", rf.me, rf.currentTerm)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.voteCh = make(chan bool)
	rf.appendCh = make(chan bool)
	rf.timeout = time.Duration(rand.Intn(150))*time.Millisecond + appendEntriesTimout*3
	rf.timer = time.NewTimer(rf.timeout)

	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				case <-rf.timer.C:
					DPrintf("[%v] Follower timeout", rf.me)
					rf.mu.Lock()
					rf.switchStateTo(Candidate)
					rf.mu.Unlock()
					rf.leaderElection()
				case <-rf.appendCh:
					DPrintf("<Follower [%v] reset election timer, bcoz appendEntry", rf.me)
					rf.timer.Reset(rf.timeout)
					DPrintf(">Follower [%v] reset election timer, bcoz appendEntry", rf.me)
				case <-rf.voteCh:
					DPrintf("Follower [%v] reset election timer, bcoz vote", rf.me)
					rf.timer.Reset(rf.timeout)
				}

			case Candidate:
				select {
				case <-rf.timer.C:
					DPrintf("Candidate [%v] timeout, re-election", rf.me)
					rf.leaderElection()
				case <-rf.appendCh:
					DPrintf("Candidate [%v] reset election timer", rf.me)
					rf.timer.Reset(rf.timeout)
					rf.mu.Lock()
					rf.switchStateTo(Follower)
					rf.mu.Unlock()
				default:
					rf.mu.Lock()
					if rf.voteCount > len(rf.peers)/2 {
						rf.switchStateTo(Leader)
					}
					rf.mu.Unlock()
				}
			case Leader:
				DPrintf("Leader [%v] reset hearbeat timeout", rf.me)
				rf.logReplication()
				time.Sleep(appendEntriesTimout)
			}
			// if rf.killed() {
			// 	DPrintf("[%v] is killed", rf.me)
			// }

		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
