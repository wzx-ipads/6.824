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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.824-golabs-2020/src/labrpc"
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

const (
	Leader = iota + 1
	Candidate
	Follower
)

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
	currentTerm int
	votedFor    int
	state       int32

	defaultElectionTimeout int32
	electionTimeout        int32
	heartbeatInterval      time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = atomic.LoadInt32(&rf.state) == Leader
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//TODO: add more fields for log replication
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// the following three fields are used for fast backup
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		// gets killed, loop forever
		for {
		}
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// RPC call from an out-dated leader. ignore it and reply with currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.votedFor = -1
		atomic.StoreInt32(&rf.state, Follower)
		rf.currentTerm = args.Term
	} else {
		// args.Term == rf.currentTerm
		if atomic.LoadInt32(&rf.state) == Leader {
			DPrintf("Error: Two leaders in one term. me: %v and sender:%v\n", rf.me, args.LeaderId)
			for {
			}
		}
	}

	// TODO: appen entries for real
	DPrintf("%v receives heartbeat from leader %v\n", rf.me, args.LeaderId)
	atomic.StoreInt32(&rf.electionTimeout, getRandomizedTimeout()) // reset timeout according to raft paper figure 2
	// A candidate may receive an AppendEntry from the cuurent leader (i.e. winner in this round of
	// leader election). It should convert to follower immediately.
	atomic.StoreInt32(&rf.state, Follower)
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	DPrintf("%v receive RequestVote from : %v\n", rf.me, args.CandidateId)
	if rf.killed() {
		// gets killed, loop forever
		for {
		}
	}

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		DPrintf("%v rejects %v in leader election, it is out-dated: %v\n", rf.me, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// RequestVote from a higer term
		// update state
		atomic.StoreInt32(&rf.state, Follower)
		rf.currentTerm = args.Term
		atomic.StoreInt32(&rf.electionTimeout, getRandomizedTimeout())

		// vote for this candidate
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("%v votes for %v in leader election\n", rf.me, args.CandidateId)
	} else {
		// args.Term == rf.currentTerm means I have already voted for another candidate or myself
		// or i have received AppendEntries from current leader
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
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
	DPrintf("%v gets killed\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// only executed by the leader node
func (rf *Raft) sendHeartBeat(term int) {
	for {
		if rf.killed() {
			runtime.Goexit()
		}

		// send Empty AppenEntries
		if atomic.LoadInt32(&rf.state) != Leader {
			runtime.Goexit()
		}
		for server := range rf.peers {
			// if server == rf.me, sendAppendEntries will not be invoked
			// send AppendEntries to all other nodes
			if server != rf.me {
				go func(server int) {
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					args.LeaderId = rf.me
					args.Term = term
					rf.sendAppendEntries(server, &args, &reply)
				}(server)
			}
		}
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) heartbeatDetector() {
	// kick off leader election periodically if not receiving heartbeat from the leader
	for {
		if rf.killed() {
			runtime.Goexit()
		}

		// wake up every five millisecond
		if atomic.LoadInt32(&rf.state) != Leader {
			atomic.AddInt32(&rf.electionTimeout, -5)
			if atomic.LoadInt32(&rf.electionTimeout) <= 0 {
				atomic.StoreInt32(&rf.electionTimeout, getRandomizedTimeout()) // Reset election timer
				go rf.callForLeaderElection()

			}
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) callForLeaderElection() {
	// convert to candidate and call for leader election
	// the following operations refer to raft paper figure 2
	rf.mu.Lock()
	atomic.StoreInt32(&rf.state, Candidate)
	rf.currentTerm++ // Increment currentTerm

	// vote for myself
	rf.votedFor = rf.me
	var cnt int32 = 1
	var finished int32 = 0
	term := rf.currentTerm
	var totalMembershipNum int32 = int32(len(rf.peers))
	rf.mu.Unlock()

	DPrintf("%v calls for leader election, my term: %v\n", rf.me, term)
	// Send RequestVote RPCs to all other servers
	for server := range rf.peers {
		// send RequestVote RPC to all servers
		if server != rf.me {
			go func(server int, cnt *int32, finish *int32) {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.CandidateId = rf.me
				args.Term = term
				res := rf.sendRequestVote(server, &args, &reply)
				if res && reply.VoteGranted {
					atomic.AddInt32(cnt, 1)
				}
				atomic.AddInt32(finish, 1)
			}(server, &cnt, &finished)
		}
	}

	// gathering the results of VoteRequest
	becomeLeader := false
	for {
		// the order matters. If we read grantedVote first, the following situation may happen:
		//
		//
		finishedVote := atomic.LoadInt32(&finished)
		grantedVote := atomic.LoadInt32(&cnt)

		if becomeLeader == false {
			rf.mu.Lock()
			if term == rf.currentTerm && grantedVote > (totalMembershipNum/2) {
				atomic.StoreInt32(&rf.state, Leader)
				atomic.StoreInt32(&rf.electionTimeout, getRandomizedTimeout()) // Reset election timer
				becomeLeader = true
				DPrintf("%v becomes leader in term: %v\n", rf.me, rf.currentTerm)
				go rf.sendHeartBeat(rf.currentTerm)
			}
			rf.mu.Unlock()
		}

		if finishedVote == totalMembershipNum {
			break
		}
	}
}

func (rf *Raft) raftMainRoutine() {
	rf.heartbeatDetector()
}

func getRandomizedTimeout() int32 {
	return 300 + rand.Int31n(300)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	//rf.defaultElectionTimeout = getRandomizedTimeout() // election timeout: 200 to 500 milliseconds
	rf.electionTimeout = getRandomizedTimeout()
	rf.heartbeatInterval = 100 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.raftMainRoutine()
	return rf
}
