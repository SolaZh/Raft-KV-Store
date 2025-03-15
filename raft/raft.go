package raft

import (
    "math/rand"
    "sync"
    "sync/atomic"
    "time"

    "ece419/labrpc"
)

// import "fmt"

// ApplyMsg is sent by each Raft peer to its service (tester or real KV server)
// to indicate a newly-committed log entry.
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int

    // For 3D (snapshots), ignore here:
    SnapshotValid bool
    Snapshot      []byte
    SnapshotTerm  int
    SnapshotIndex int
}

// RaftState enumerates states for a server: follower, candidate, leader.
type RaftState int

const (
    Follower RaftState = iota
    Candidate
    Leader
)

type LogEntry struct {
    Command interface{}
    Term    int
}

// Raft implements a single Raft peer.
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd
    persister *Persister
    me        int
    dead      int32 // set by Kill()

    // Persistent state on all servers (Figure 2)
    currentTerm int
    votedFor    int
    log         []LogEntry

    // Volatile state on all servers (Figure 2)
    commitIndex int
    lastApplied int

    // Volatile state on leaders (Figure 2)
    nextIndex  []int
    matchIndex []int

    // Additional housekeeping
    state            RaftState
    lastHeartbeat    time.Time    // time of last heartbeat or vote
    electionTimeout  time.Duration
    applyCh          chan ApplyMsg
    applyCond        *sync.Cond   // used to signal applier goroutine
}



// RequestVoteArgs / RequestVoteReply for leader election
type RequestVoteArgs struct {
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

type RequestVoteReply struct {
    Term        int
    VoteGranted bool
}

// AppendEntriesArgs / AppendEntriesReply for heartbeats or log replication
type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}


func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, (rf.state == Leader)
}


func (rf *Raft) persist() {
    // Placeholder for Part 3C: Save Raft state (currentTerm, votedFor, log[]) to persister.
}

func (rf *Raft) readPersist(data []byte) {
    // Placeholder for Part 3C: Restore previously persisted state.
}


func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // For Part 3D. Not needed here.
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Leader {
        return -1, rf.currentTerm, false
    }

    // Leader appends the command to its log
    newIndex := len(rf.log)
    rf.log = append(rf.log, LogEntry{
        Command: command,
        Term:    rf.currentTerm,
    })

    // Update leader’s own data for nextIndex[me], matchIndex[me] if desired (often matchIndex for self is last index)
    // but strictly speaking we can skip that for the basic approach; the heartbeatLoop handles replication.

    // Persist in memory (and stable storage eventually)
    rf.persist()

    return newIndex+1, rf.currentTerm, true
}


func (rf *Raft) ticker() {
    for rf.killed() == false {
        rf.mu.Lock()
        isLeader := (rf.state == Leader)
        timeSinceHeartbeat := time.Since(rf.lastHeartbeat)
        electionTimeout := rf.electionTimeout
        rf.mu.Unlock()

        if !isLeader && timeSinceHeartbeat >= electionTimeout {
            // Start election
            rf.startElection()
        }

        // Sleep a short random duration to avoid tight loops
        ms := 50 + (rand.Int63() % 50)
        time.Sleep(time.Duration(ms) * time.Millisecond)
    }
}


func (rf *Raft) startElection() {
    rf.mu.Lock()
    if rf.state == Leader {
        // If we’re already leader, no need to start election
        rf.mu.Unlock()
        return
    }

    rf.state = Candidate
    rf.currentTerm++
    termAtStart := rf.currentTerm
    rf.votedFor = rf.me
    votes := 1
    rf.lastHeartbeat = time.Now() // reset to avoid immediate re-election
    lastLogIndex := len(rf.log) - 1
    lastLogTerm := 0
    if lastLogIndex >= 0 {
        lastLogTerm = rf.log[lastLogIndex].Term
    }
    rf.mu.Unlock()

    // Send RequestVote RPCs in parallel
    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go func(peer int) {
            args := RequestVoteArgs{
                Term:         termAtStart,
                CandidateId:  rf.me,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }
            var reply RequestVoteReply

            ok := rf.sendRequestVote(peer, &args, &reply)
            if ok {
                rf.mu.Lock()
                defer rf.mu.Unlock()

                if rf.state != Candidate || rf.currentTerm != termAtStart {
                    // If we aren’t candidate or have moved on in term,
                    // ignore the response
                    return
                }
                if reply.Term > rf.currentTerm {
                    // Found a higher term => step down
                    rf.currentTerm = reply.Term
                    rf.state = Follower
                    rf.votedFor = -1
                    rf.persist()
                    return
                }
                if reply.VoteGranted {
                    votes++
                    if votes > len(rf.peers)/2 && rf.state == Candidate {
                        // We have majority => become leader
                        rf.state = Leader
                        // Initialize leader state
                        rf.initLeaderStateLocked()
                        // Start sending heartbeats
                        go rf.heartbeatLoop()
                    }
                }
            }
        }(i)
    }
}

func (rf *Raft) heartbeatLoop() {
    for {
        rf.mu.Lock()
        if rf.state != Leader {
            rf.mu.Unlock()
            return
        }
        term := rf.currentTerm
        rf.mu.Unlock()

        // Send AppendEntries to every peer
        for i := range rf.peers {
            if i == rf.me {
                continue
            }
            go rf.sendAppendEntriesToPeer(i, term)
        }
        time.Sleep(100 * time.Millisecond) // Heartbeat interval
    }
}

// sendAppendEntriesToPeer handles the logic of sending new log entries to a follower (peerId).
// This function is called in the heartbeat loop while holding no lock; it reacquires the lock inside.
func (rf *Raft) sendAppendEntriesToPeer(peerId int, leaderTerm int) {
    rf.mu.Lock()
    // Double-check if we’re still leader, same term
    if rf.state != Leader || rf.currentTerm != leaderTerm {
        rf.mu.Unlock()
        return
    }

    // Prepare arguments
    prevIndex := rf.nextIndex[peerId] - 1
    prevTerm := 0
    if prevIndex >= 0 {
        prevTerm = rf.log[prevIndex].Term
    }
    entries := make([]LogEntry, 0)
    // We send all log entries from nextIndex[peerId] onward
    if rf.nextIndex[peerId] < len(rf.log) {
        entries = rf.log[rf.nextIndex[peerId]:]
    }

    args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: prevIndex,
        PrevLogTerm:  prevTerm,
        Entries:      entries,
        LeaderCommit: rf.commitIndex,
    }
    rf.mu.Unlock()

    // Send RPC
    var reply AppendEntriesReply
    ok := rf.sendAppendEntries(peerId, &args, &reply)
    if !ok {
        return
    }

    // Process reply
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // If our term is stale, step down
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.state = Follower
        rf.votedFor = -1
        rf.persist()
        return
    }

    // If still leader and term unchanged
    if rf.state == Leader && leaderTerm == rf.currentTerm {
        if reply.Success {
            // Advance nextIndex and matchIndex
            // nextIndex = old nextIndex + len(entries) we successfully appended
            matchLen := (prevIndex + 1) + len(entries)
            rf.matchIndex[peerId] = matchLen - 1
            rf.nextIndex[peerId] = matchLen

            // Now try to update commitIndex if there is a log index that is replicated
            // on a majority of servers and is in our currentTerm.
            for i := rf.commitIndex + 1; i < len(rf.log); i++ {
                if rf.log[i].Term == rf.currentTerm {
                    count := 1
                    for p := range rf.peers {
                        if rf.matchIndex[p] >= i {
                            count++
                        }
                    }
                    if count > len(rf.peers)/2 {
                        rf.commitIndex = i
                    }
                }
            }
            // Signal the applier goroutine if commitIndex advanced
            rf.applyCond.Signal()

        } else {
            // If AppendEntries fails, it usually means a mismatch => decrement nextIndex
            if rf.nextIndex[peerId] > 1 {
                rf.nextIndex[peerId]--
            }
        }
    }
}

func (rf *Raft) initLeaderStateLocked() {
    lastIndex := len(rf.log)
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i := range rf.peers {
        rf.nextIndex[i] = lastIndex
        rf.matchIndex[i] = -1
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    // If the term is newer, update ourselves
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = Follower
        rf.votedFor = -1
        rf.persist()
    }

    // At this point, terms match => treat it as a valid leader heartbeat
    rf.lastHeartbeat = time.Now()

    reply.Term = rf.currentTerm

    // Check log consistency with PrevLogIndex/PrevLogTerm
    if args.PrevLogIndex >= 0 {
        if args.PrevLogIndex >= len(rf.log) {
            // We don't even have PrevLogIndex in local log => fail
            reply.Success = false
            return
        }
        if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
            // Mismatch in term => fail
            reply.Success = false
            return
        }
    }

    // If we reach here, logs match => accept new entries
    // Overwrite conflicting entries
    insertPos := args.PrevLogIndex + 1
    for i, entry := range args.Entries {
        if insertPos+i < len(rf.log) {
            // If there’s a conflict
            if rf.log[insertPos+i].Term != entry.Term {
                // Truncate the existing conflict and append
                rf.log = rf.log[:insertPos+i]
                rf.log = append(rf.log, entry)
            }
        } else {
            // The log is shorter, just append
            rf.log = append(rf.log, entry)
        }
    }
    reply.Success = true

    // Update commitIndex if leaderCommit > commitIndex
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
        rf.applyCond.Signal()
    }
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = Follower
        rf.votedFor = -1
        rf.persist()
    }

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    // Check if we have already voted or if the candidate is more up-to-date
    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        // Compare candidate's last log index/term with our own
        lastIndex := len(rf.log) - 1
        lastTerm := 0
        if lastIndex >= 0 {
            lastTerm = rf.log[lastIndex].Term
        }

        // Candidate's log must be at least as up-to-date
        upToDate := (args.LastLogTerm > lastTerm) ||
            (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

        if upToDate {
            rf.votedFor = args.CandidateId
            rf.lastHeartbeat = time.Now() // treat as hearing from a "leader" for the term
            reply.VoteGranted = true
            rf.persist()
        }
    }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}


func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
    return atomic.LoadInt32(&rf.dead) == 1
}


func (rf *Raft) applier() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    for !rf.killed() {
        // Wait until commitIndex > lastApplied
        for rf.commitIndex <= rf.lastApplied {
            rf.applyCond.Wait()
            if rf.killed() {
                return
            }
        }

        // Now apply all entries between lastApplied+1 and commitIndex inclusive
        rf.lastApplied++
        idx := rf.lastApplied
		// fmt.Printf("[Server %d] apply log[%d], cmd=%v (commitIndex=%d)\n",
        //    rf.me, idx, rf.log[idx].Command, rf.commitIndex)

        msg := ApplyMsg{
            CommandValid: true,
            Command:      rf.log[idx].Command,
            CommandIndex: idx + 1, // +1 if your tester expects 1-based
        }

        rf.mu.Unlock()
        rf.applyCh <- msg
        rf.mu.Lock()
    }
}


func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {

    rf := &Raft{
        peers:     peers,
        persister: persister,
        me:        me,
        applyCh:   applyCh,

        currentTerm: 0,
        votedFor:    -1,
        state:       Follower,
        commitIndex: -1,
        lastApplied: -1,
    }

    // Random election timeout in [300..500] ms
    rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
    rf.lastHeartbeat = time.Now()

    // Read any previously persisted state
    rf.readPersist(persister.ReadRaftState())

    // Initialize leader arrays. They’re only used when we become leader, but
    // let’s allocate them for safety.
    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    // Start ticker goroutine
    go rf.ticker()

    // Start a separate applier goroutine
    rf.applyCond = sync.NewCond(&rf.mu)
    go rf.applier()

    return rf
}


// Helper: min function

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
