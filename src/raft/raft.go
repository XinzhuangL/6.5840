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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role              Role
	lastHeartbeatTime time.Time
	electionStartTime time.Time

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntity

	stateMatch interface{}

	// Volatile state on all servers
	commitIndex int // 最新的已知已经被提交的日志索引(添加到[])
	lastApplied int // 最新的被应用于状态机的日志索引(应用到状态机)

	// Volatile state on leader
	nextIndex  []int // leader发个每个server最后日志对象的索引 (初始化为leader上一个日志索引+1)
	matchIndex []int // 已知的已经被复制的最高的日志对象索引(初始化为0，单调递增)
}

type AppendEntriesArgs struct {
	Term              int         // leader任期
	LeaderId          int         // leaderId
	PrevLogIndex      int         // 上一个日志条目的索引
	PrevLogTerm       int         // 上一个日志条目的任期  用于一致性比对
	Entries           []LogEntity // append的日志条目，可以一次发送多个
	LeaderCommitIndex int         // 最新的leader已经被提交的日志索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，如果大于leader任期，当前leader转变为follower
	Success bool // 是否成功，是否匹配prevLogIndex和prevLogTerm
}

type LogEntity struct {
	command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.printKeyInfo("GetState")
	return term, isleader
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
	Term         int // 候选人任期
	CandidateId  int // 候选人Id
	LastLogIndex int // 最新日志索引
	LastLogTerm  int // 最新日志所在的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 当前任期，为候选人更新自己
	VoteGranted bool // if true，意味候选人接收到了投票
}

// mutex in out func
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/*
		接受远端的投票请求
		校验
		1. term 大于当前 term
		2. log比当前新

	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	// if args.term < currTerm return false
	// if args.lastLogIndex < this.log.len return false
	// if args.lastLogTerm < this.log.term return false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.LastLogIndex < rf.getLastLogIndex() {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.LastLogTerm < rf.getLastLogTerm() {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.printKeyInfo("find a new leader by RequestVote, transform to follower")
		rf.role = Follower
	}
	reply.VoteGranted = true
	// we update currentTerm = request term
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	return
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

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/* 3A
	接受远端添加日志的请求
	需要term 大于当前term
	如果是心跳请求 需要更新最后接收到心跳的时间，需要更新当前任期，如果当前角色为leader 还需要转变为follower
	*/
	// todo impl this in 3B
	/* 3B
	1. if term < currentTerm return false
	2. if PrevLogIndex != this.log.PreLogIndex || PrevLogTerm != this.log.PrevLogTerm return false
	3. if PreLogIndex = this.log.PreLogIndex && PreLogTerm > this.log.PrevLogTerm append new
	4. append not exist
	5. if leaderCommit > commitIndex set commitIndex = min(leaderCommit, index of last new entry)

	client request Or leader request


	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// discard
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Entries == nil {
		// todo include commitIndex, use it to apply
		// heartbeat
		rf.lastHeartbeatTime = time.Now()
		if args.Term > rf.currentTerm {
			rf.printKeyInfo("find a new leader by heartbeat, transform to follower")
			rf.role = Follower
		}
		rf.currentTerm = args.Term
		rf.printKeyInfo("receive a heart " + rf.lastHeartbeatTime.String())
	} else {
		preLogIndex := rf.getLastLogIndex()
		preLogTerm := rf.getLastLogTerm()
		// 2. if PrevLogIndex != this.log.PreLogIndex || PrevLogTerm != this.log.PrevLogTerm return false
		if preLogIndex != rf.getLastLogIndex() || preLogTerm != rf.getLastLogTerm() {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		// 3. if PreLogIndex = this.log.PreLogIndex && PreLogTerm > this.log.PrevLogTerm append new
		if preLogIndex == rf.getLastLogIndex() && preLogTerm > rf.getLastLogTerm() {
			// todo process once only
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.log[rf.getLastLogIndex()] = args.Entries[len(args.Entries)-1]
			return
		}
		// 4. append not exists
		logs := args.Entries
		for i := 0; i < len(logs); i++ {
			rf.log = append(rf.log, logs[i])
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		// 5. if leaderCommit > commitIndex set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastLogIndex())
		}

	}
	// other impl
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// multiple send log
// todo serial now
//func (rf *Raft) multipleReplicaLog(startIdx int, endIdx int) (map[int]*AppendEntriesArgs, map[int]*AppendEntriesReply) {
//	// save args & reply
//	idToArgs := make(map[int]*AppendEntriesArgs)
//	idToReply := make(map[int]*AppendEntriesReply)
//
//	for i := 0; i < len(rf.peers); i++ {
//		if i == rf.me {
//			continue
//		}
//		subNewEntries := rf.log[startIdx : endIdx+1]
//		newEntries := make([]LogEntity, len(subNewEntries))
//		copy(newEntries, subNewEntries)
//		args := &AppendEntriesArgs{
//			Term:              rf.currentTerm,
//			LeaderId:          rf.me,
//			PrevLogIndex:      rf.getLastLogIndex(),
//			PrevLogTerm:       rf.getLastLogTerm(),
//			Entries:           newEntries,
//			LeaderCommitIndex: rf.getLastLogIndex(),
//		}
//		reply := &AppendEntriesReply{}
//		rf.sendAppendEntries(i, args, reply)
//		idToArgs[i] = args
//		idToReply[i] = reply
//	}
//	// todo maybe failed
//	return idToArgs, idToReply
//}

// todo replicaOne
func (rf *Raft) multipleReplicaLon(index int) (map[int]*AppendEntriesArgs, map[int]*AppendEntriesReply) {
	idToArgs := make(map[int]*AppendEntriesArgs)
	idToReply := make(map[int]*AppendEntriesReply)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		idToArgs[i], idToReply[i] = rf.replicaLog(i, index)
	}

	return idToArgs, idToReply
}

func (rf *Raft) asyncMultipleReplicaLog(index int) (chan result, chan error) {
	results := make(chan result, len(rf.peers)-1)
	errors := make(chan error, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args, reply := rf.replicaLog(server, index)
			results <- result{rf.me, args, reply}
			errors <- nil
		}(i)
	}

	// clear
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func() {
				defer wg.Done()
				<-errors
			}()

			go func() {
				defer wg.Done()
				<-results
			}()
		}
		wg.Wait()
		close(results)
		close(errors)
	}()
	return results, errors
}

type result struct {
	server      int
	appendArgs  *AppendEntriesArgs
	appendReply *AppendEntriesReply
}

func (rf *Raft) replicaLog(server int, index int) (*AppendEntriesArgs, *AppendEntriesReply) {
	entity := rf.log[index]
	args := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      rf.getLastLogIndex(),
		PrevLogTerm:       rf.getLastLogTerm(),
		Entries:           []LogEntity{entity},
		LeaderCommitIndex: rf.getLastLogIndex(),
	}

	reply := &AppendEntriesReply{}
	rf.sendAppendEntries(server, args, reply)
	return args, reply
}

// heartbeat
func (rf *Raft) sendHeartbeat(server int, reply *AppendEntriesReply) bool {
	args := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      rf.getLastLogIndex(),
		PrevLogTerm:       rf.getLastLogTerm(),
		Entries:           nil,
		LeaderCommitIndex: rf.commitIndex,
	}
	return rf.sendAppendEntries(server, args, reply)
}

// replica log
//func (rf *Raft) sendReplicaLog(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//
//}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.printKeyInfo("send heart to id: " + strconv.Itoa(server) + ", args.LeaderId: " + strconv.Itoa(args.LeaderId) + ", args.Term: " + strconv.Itoa(args.Term))
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
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (3B).
	// 在这里处理客户端请求的命令
	// 需要完成完整的请求流程
	// 1. 接受请求
	// leader从client接收到包含新的日志条目的请求
	entry := &LogEntity{
		command: command,
		Term:    term,
	}

	// todo 需要lock嘛
	// 2.添加条目到本地log
	// leader将这个新的日志条目添加到本地，赋予它当前term和一个连续的index
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, *entry)

	// 3.发送AppendEntries RPC
	// leader像所有其他节点发送一个AppendEntriesRPC，请求他们复制这个新的日志条目
	// multiple rpc to all follower

	// todo current last one
	results, _ := rf.asyncMultipleReplicaLog(rf.getLastLogIndex())
	// todo failed need to recover

	// 4.等待大多数节点响应
	// leader等待大多数节点响应AppendEntries RPC，表示follower已将其复制到本地的日志条目中
	i := 0
	isSuccess := false
	for each := range results {
		if each.appendReply.Success {
			i++
			if i >= len(rf.peers) {
				isSuccess = true
				break
			}
		}
	}

	if !isSuccess {
		log.Printf(" start command error")
		return index, term, isLeader
	}

	// 5.更新commitIndex
	// leader收到大多数节点的成功响应后，更新自己的commitIndex，反应最新提交的日志条目
	rf.commitIndex = rf.getLastLogIndex()

	// 6. 更新条目到状态机
	// leader将commitIndex之前所有的日志条目，应用到自己的状态机中。顺序执行，并且更新lastApplied

	// from lastApplied -> rf.commitIndex
	rf.stateMatch = entry.command.(string)
	rf.lastApplied = rf.commitIndex

	// 7. 响应客户端请求
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

func (rf *Raft) checkConsistent() {
	// check consistent
	// 1. check commitIndex >= follower的nextIndex，从nextIndex开始发送给follower
	// 1.1 success 更新nextIndex和matchIndex
	// 1.2 failed 减少nextIndex 重试
	for i := 0; i < len(rf.nextIndex); i++ {
		if i == rf.me {
			continue
		}
		if rf.commitIndex >= rf.nextIndex[i] {
			log.Printf("commit index: %d is greater than next index %d:%d, start sync", rf.commitIndex, i, rf.nextIndex[i])

		}
	}
}

// commitIndex >= nextIndex  we sync nextIndex -> to commitInd
func (rf *Raft) syncLog(server int, commitIndex int, nextIndex int) {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		for commitIndex >= nextIndex {
			_, replay := rf.replicaLog(server, nextIndex)
			if replay.Success {
				rf.matchIndex[server] = nextIndex
			} else {
				nextIndex--
			}
			log.Printf("try to sync log %d ==> %d: commit index: %d, nextIndex: %d", rf.me, server, commitIndex, nextIndex)
		}

	}()
}

func (rf *Raft) ticker() {
	/*
		实现周期性调度逻辑
		1. Leader时，定时向follower发送心跳
		返回的term 大于当前term时，以为该leader  已经失效了 需要转变为follower
		2. Follower时
		检测心跳，超时 转变为Candidate，并请求投票
		3. Candidate时
		检测 选举是否超时，超时开启新一轮选举
	*/

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// check role
		// heartbeat less than 5 times per second
		if Leader == rf.role {
			rf.printKeyInfo("trick to leader")
			// send heartbeat
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.printKeyInfo("try to send heart to " + strconv.Itoa(i))
				reply := &AppendEntriesReply{}
				rf.sendHeartbeat(i, reply)
				// if reply == false we need change role
				if !reply.Success {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.currentTerm = reply.Term
					}
					rf.printKeyInfo("heartbeat error")
					rf.mu.Unlock()
					break
				}
			}
			// sync checkConsistent
			rf.checkConsistent()

			// sleep
			time.Sleep(time.Millisecond * 100)
		} else if Follower == rf.role {
			rf.printKeyInfo("trick to follow")
			// check heartbeat timeout
			if time.Now().Sub(rf.lastHeartbeatTime) > time.Millisecond*150 {
				/*
					随机开始选举时间
				*/
				ms := 150 + (rand.Int63() % 150)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				// change to candidate and start a election
				rf.mu.Lock()
				rf.printKeyInfo("start to elect leader")
				if time.Now().Sub(rf.lastHeartbeatTime) <= time.Millisecond*150 {
					// have a new leader
					rf.mu.Unlock()
					rf.printKeyInfo("have a new leader")
					continue
				}
				rf.role = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.electionStartTime = time.Now()
				rf.mu.Unlock()
				rf.printKeyInfo("try to send requestVotes")
				rf.requestVotes()

			} else {
				time.Sleep(time.Millisecond * 100)
			}
		} else if Candidate == rf.role {
			rf.printKeyInfo("trick to candidate")
			ms := 150 + (rand.Int63() % 150)
			// check election time out
			/*
				随机选举超时时间
			*/
			if time.Now().Sub(rf.lastHeartbeatTime) > time.Duration(ms)*time.Millisecond {
				rf.mu.Lock()
				rf.role = Follower
				rf.mu.Unlock()
			}

		}
	}
	rf.printKeyInfo("trick finished")
}

func (rf *Raft) printKeyInfo(appendInfo string) {
	//strRole := ""
	//if rf.role == 0 {
	//	strRole = "LEADER"
	//}
	//if rf.role == 1 {
	//	strRole = "FOLLOWER"
	//}
	//if rf.role == 2 {
	//	strRole = "CANDIDATE"
	//}

	// log.Printf("raft id: %d, term: %d, role: %s, info: %s", rf.me, rf.currentTerm, strRole, appendInfo)
}

func (rf *Raft) requestVotes() {
	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
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
	// Wait for all goroutines to finish
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-ch
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
		}
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
	rf.role = Follower

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
