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
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type Role int

const (
	Leader    Role = 0
	Follower  Role = 1
	Candidate Role = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	role              Role
	lastHeartbeatTime time.Time
	electionStartTime time.Time

	currentTerm int
	votedFor    int
	logs        []*LogEntity

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntity struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isLeader := rf.role == Leader
	// Your code here (3A).
	return term, isLeader
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
	Term         int // 候选人任期
	CandidateId  int // 候选人Id
	LastLogIndex int // 最新日志索引
	LastLogTerm  int // 最新日志所在的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，为候选人更新自己
	VoteGranted bool // 投票则为true
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PreLogIndex       int
	PreLogTerm        int
	Entries           []*LogEntity
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// receive a vote request
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		rf.printKeyInfo("we received a vote, but less current term")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor < 0 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	} else {
		rf.printKeyInfo("we received a valid  vote request args: Term: " + strconv.Itoa(args.Term) + " args: Candidate: " + strconv.Itoa(args.CandidateId))
		currentTerm := rf.currentTerm
		rf.currentTerm = args.Term
		reply.Term = currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		if rf.role == Leader {
			rf.role = Follower
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// append logs | heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Entries == nil {
		// we receive a heartbeat
		rf.lastHeartbeatTime = time.Now()
		if args.Term > rf.currentTerm {
			rf.printKeyInfo("find a new leader by heartbeat, transform to follower")
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = -1
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.printKeyInfo("receive a heart arg.Term: " + strconv.Itoa(args.Term) + " args.LeadId: " + strconv.Itoa(args.LeaderId))
	} else {
		// todo append log
	}
}

// send heartbeat to every
func (rf *Raft) multipleSendHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
			if !reply.Success {
				rf.mu.Lock()
				rf.printKeyInfo("heartbeat error")
				if reply.Term > rf.currentTerm {
					rf.printKeyInfo("heartbeat to follower")
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}

		}(i)
	}
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

// send AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (3B).

	return index, term, isLeader
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

//func (rf *Raft) printKeyInfo(appendInfo string) {
//	strRole := ""
//	if rf.role == 0 {
//		strRole = "LEADER"
//	}
//	if rf.role == 1 {
//		strRole = "FOLLOWER"
//	}
//	if rf.role == 2 {
//		strRole = "CANDIDATE"
//	}
//
//	log.Printf("raft id: %d, term: %d, role: %s, info: %s", rf.me, rf.currentTerm, strRole, appendInfo)
//}

func (rf *Raft) printKeyInfo(appendInfo string) {}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		switch rf.role {
		case Leader:
			rf.leaderTicker()
			break
		case Follower:
			rf.followerTicker()
			break
		default:
			rf.printKeyInfo("we reach a impossible position")
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) leaderTicker() {
	// epoch
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if Leader == rf.role {
		rf.multipleSendHeartbeat()
		// TODO: checkConsistent ?
	}
}

func (rf *Raft) followerTicker() {
	// > 150ms start a elect
	if time.Now().Sub(rf.lastHeartbeatTime) > 150*time.Millisecond {
		// random start elect
		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// change to candidate and start a elect
		rf.mu.Lock()

		// double check heartbeat
		if time.Now().Sub(rf.lastHeartbeatTime) <= time.Millisecond*150 {
			// receive heartbeat
			rf.mu.Unlock()
			rf.printKeyInfo("have a new leader or receive a heartbeat")
			return
		}
		// change state and prepare a request
		rf.role = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.printKeyInfo("start to elect leader")
		// rf.electionStartTime = time.Now()

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm:  rf.getLastLogTerm(),
		}
		rf.mu.Unlock()
		rf.requestVotes(args)

	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getIndexLogTerm(index int) int {
	if index < 0 {
		return 0
	}
	return rf.logs[index].Term
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getIndexLogTerm(len(rf.logs) - 1)
}

func (rf *Raft) requestVotes(args *RequestVoteArgs) {
	// async to send
	var ch = make(chan *RequestVoteReply)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply)
			ch <- reply
		}(i)
	}

	ct := 0
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case reply := <-ch:
			if reply.VoteGranted {
				ct++
				if ct >= len(rf.peers)/2 {
					// win the elect
					rf.mu.Lock()
					if rf.role != Candidate {
						rf.mu.Unlock()
						return
					}
					rf.role = Leader
					rf.printKeyInfo("change to new leader")
					rf.mu.Unlock()
					return
				}
			} else {
				rf.printKeyInfo("reply : " + strconv.Itoa(reply.Term) + " " + strconv.FormatBool(reply.VoteGranted))
			}
		case <-time.After(150 * time.Millisecond):
			rf.printKeyInfo("vote timeout")
			rf.mu.Lock()
			rf.role = Follower
			rf.mu.Unlock()
			// time.Sleep(time.Duration(100 + (rand.Int63() % 100)))
			return
		}
	}
	// we receive all but not elect success
	rf.printKeyInfo("vote unsuccess")
	rf.mu.Lock()
	rf.role = Follower
	rf.mu.Unlock()
	return
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
	rf.votedFor = -1
	rf.role = Follower

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
