// collabrator rash, mussie
package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"

	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
	"math/rand"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type LogEntry struct {
	Term    int
	Command interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex        []int
	matchIndex       []int
	heartbeattimeout *time.Timer
	Electiontimeout  *time.Timer

	state string // "follower", "candidate", "leader"

	applyCh chan ApplyMsg
	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	isleader = rf.state == "leader"
	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.heartbeattimeout.Stop()
		rf.Electiontimeout.Reset(time.Duration(rand.Intn(300)+601) * time.Millisecond)
	}

	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	entryIndex := args.PrevLogIndex + 1

	for i, entry := range args.Entries {
		if entryIndex+i < len(rf.log) {

			if rf.log[entryIndex+i].Term != entry.Term {
				rf.log = rf.log[:entryIndex+i]
				break
			}
		} else {
			break
		}
	}

	if entryIndex < len(rf.log) {
		rf.log = append(rf.log[:entryIndex], args.Entries...)
	} else {
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {

			rf.commitIndex = len(rf.log) - 1
		}

		rf.applyEntry()

	}

	rf.Electiontimeout.Reset(time.Duration(rand.Intn(300)+601) * time.Millisecond)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok && reply.Success {
		rf.mu.Lock()
		//update index
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		N := args.PrevLogIndex + len(args.Entries)
		count := 0
		if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
			for peer := range rf.peers {
				if rf.matchIndex[peer] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N

				rf.mu.Unlock()
				rf.applyEntry()
				rf.mu.Lock()
			}
		}

		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.nextIndex[server] = 1
		rf.mu.Unlock()
	}

	return ok
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int
	VoteGranted bool
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	isLogUpToDate := args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) //for second conditional

	if args.Term < rf.currentTerm {

		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.heartbeattimeout.Stop()
		rf.Electiontimeout.Reset(time.Duration(rand.Intn(300)+601) * time.Millisecond) //because there is a follwoer who didnt vote for that leader
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogUpToDate { //second
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.Electiontimeout.Reset(time.Duration(rand.Intn(300)+601) * time.Millisecond) //because the follower voted
	}

}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.state == "leader")

	// Your code here (4B).

	term = rf.currentTerm
	if !isLeader {
		isLeader = false
		return index, -1, false
	}
	index = len(rf.log)
	rf.matchIndex[rf.me] = index
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})

	rf.persist()
	return index, term, true
}

func (rf *Raft) applyEntry() {

	if rf.lastApplied < rf.commitIndex {

		log := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)
		go func(lastApplied int, log_entry []LogEntry) {
			for i := 0; i < len(log_entry); i++ {
				Msg := ApplyMsg{CommandValid: true, Command: log_entry[i].Command, CommandIndex: lastApplied + i}
				rf.applyCh <- Msg
				rf.mu.Lock()
				if rf.lastApplied < Msg.CommandIndex {
					rf.lastApplied = Msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, log)
	}
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		currentState := rf.state
		rf.mu.Unlock()

		switch currentState {
		case "follower", "candidate":
			select {
			case <-rf.Electiontimeout.C:
				rf.mu.Lock()
				if rf.state == "candidate" {
					rf.mu.Unlock()
					rf.NewElection()
				} else if rf.state == "follower" {
					rf.state = "candidate"
					rf.mu.Unlock()
					rf.NewElection()
				} else {
					rf.mu.Unlock()
				}
			}
		case "leader":
			select {
			case <-rf.heartbeattimeout.C:
				if rf.state == "leader" {

					// otherwise loop through every peer and start goroutines for all of them
					for i := range rf.peers {
						if i != rf.me {
							go func(server int) {

								rf.mu.Lock()
								if rf.state != "leader" {
									rf.mu.Unlock()
									return
								}
								//fmt.Println("before", rf.nextIndex, rf.nextIndex[peer])
								logEntries := rf.log[(rf.nextIndex[server]):]

								args := &AppendEntriesArgs{
									Term:         rf.currentTerm,
									LeaderId:     rf.me,
									PrevLogIndex: rf.nextIndex[server] - 1,
									PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
									Entries:      logEntries,
									LeaderCommit: rf.commitIndex,
								}
								rf.mu.Unlock()

								reply := &AppendEntriesReply{}
								rf.sendAppendEntries(server, args, reply)

							}(i)
						}
						time.Sleep(10 * time.Millisecond)
					}
					rf.heartbeattimeout.Reset(100 * time.Millisecond)
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) NewElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = "candidate"
	currentTerm := rf.currentTerm
	rf.Electiontimeout.Reset(time.Duration(rand.Intn(300)+601) * time.Millisecond)
	rf.mu.Unlock()
	votes := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()

			if rf.state != "candidate" || rf.killed() {
				rf.mu.Unlock()
				return
			}

			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}

			//send request vote
			ok := rf.sendRequestVote(server, args, reply)
			// fmt.Println("aquired lock")
			if ok {
				rf.mu.Lock()
				if reply.Term < rf.currentTerm || rf.state != "candidate" || args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {

					votes++
					if votes > len(rf.peers)/2 {
						if rf.state == "leader" {
							rf.mu.Unlock()
							return
						}
						rf.state = "leader"
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}
						for i := range rf.matchIndex {
							rf.matchIndex[i] = 0
						}

						rf.mu.Unlock()
						rf.Electiontimeout.Stop()
						if rf.state != "leader" {
							return
						}

						for peer := range rf.peers {
							if peer != rf.me {
								go func(peer int) {

									rf.mu.Lock()
									if rf.state != "leader" {
										rf.mu.Unlock()
										return
									}

									Entry := rf.log[(rf.nextIndex[peer]):]

									args := &AppendEntriesArgs{
										Term:         rf.currentTerm,
										LeaderId:     rf.me,
										PrevLogIndex: rf.nextIndex[peer] - 1,
										PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
										Entries:      Entry,
										LeaderCommit: rf.commitIndex,
									}
									rf.mu.Unlock()

									reply := &AppendEntriesReply{}
									rf.sendAppendEntries(peer, args, reply)

								}(peer)
							}
							time.Sleep(10 * time.Millisecond)
						}
						rf.mu.Lock()
						rf.heartbeattimeout.Reset(100 * time.Millisecond)
					}

				}
				rf.mu.Unlock()
			}

		}(i)

	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
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
	rf.applyCh = applyCh
	rf.me = me
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartbeattimeout = time.NewTimer(100 * time.Millisecond)
	rf.Electiontimeout = time.NewTimer(time.Duration(rand.Intn(300)+601) * time.Millisecond) //random timer between 1000-1500

	// Your initialization code here (4A, 4B).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 indicates no vote

	rf.state = "follower"

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()
	return rf
}
