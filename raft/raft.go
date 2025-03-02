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
// rf.GetState() (term, isleader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"ece419/labgob"
	"ece419/labrpc"
)



// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State on all servers
	currentTerm int
	votedFor int

	// Volatile state
	state         RaftState
	lastHeartbeat time.Time // time of last heartbeat (or vote) received
	// electionTimeout is randomized (e.g., between 300-500 ms)
	electionTimeout time.Duration

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
    Term     int
    LeaderId int
    // Next labs use: PrevLogIndex, PrevLogTerm, Entries, LeaderCommit
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If candidate's term is older, reject vote.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// If the candidate's term is higher, update our term and convert to follower.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Grant vote if we haven't voted for anyone in this term or already voted for this candidate.
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// Reset the heartbeat timer.
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If leader's term is older, reject.
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Otherwise, update term and convert to follower.
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.votedFor = -1
	// Reset the heartbeat timer since we've heard from a valid leader.
	rf.lastHeartbeat = time.Now()
	reply.Success = true
	reply.Term = rf.currentTerm
}


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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}



func (rf *Raft) startElection() {
	rf.mu.Lock()
	// Become Candidate.
	rf.state = Candidate
	rf.currentTerm++
	termAtStart := rf.currentTerm
	rf.votedFor = rf.me
	votes := 1 // vote for self
	rf.mu.Unlock()

	// Send RequestVote RPCs to all other peers.
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{
				Term:        termAtStart,
				CandidateId: rf.me,
			}
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// If the candidate has been superseded or stepped down, do nothing.
				if rf.state != Candidate || termAtStart != rf.currentTerm {
					return
				}
				// If the reply has a higher term, step down.
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					return
				}
				if reply.VoteGranted {
					votes++
					// If we have a majority, become leader.
					if votes > len(rf.peers)/2 && rf.state == Candidate {
						rf.state = Leader
						// Start sending heartbeats.
						go rf.heartbeatLoop()
					}
				}
			}
		}(i)
	}
}

// heartbeatLoop is run by the leader to send periodic heartbeats.
func (rf *Raft) heartbeatLoop() {
	for {
		rf.mu.Lock()
		// Only continue heartbeating if still leader.
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		// Send heartbeat to every peer.
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{
					Term:     term,
					LeaderId: rf.me,
				}
				var reply AppendEntriesReply
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					// If a follower responds with a higher term, step down.
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(50 * time.Millisecond) // heartbeat interval (adjust as needed)
	}
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	// For Part 3A, we are not implementing log replication.
	// Simply return a dummy index (-1) and current term.
	return -1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// If not the leader and enough time has passed since the last heartbeat,
		// start a new election.
		if rf.state != Leader && time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
		// Sleep a short random duration to avoid tight looping.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialize state.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	// Randomized election timeout between 300 and 500 milliseconds.
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond

	// Initialize from persisted state, if any.
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine.
	go rf.ticker()

	return rf
}
