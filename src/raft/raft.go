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
	"fmt"
	"log"
	"sort"

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
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// entry
type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfChange
	EntryHeartbeat
)

type Entry struct {
	EntryType EntryType
	Term      int
	Index     int
	Data      []byte // 暂定这个类型 对应proc类型为 bytes
}

type RaftLog struct {
	mu       sync.RWMutex
	firstIdx int
	lastIdx  int
	// todo a kv store
	// temp use a array
	entries []Entry
}

func (rl *RaftLog) GetFirst() *Entry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return &rl.entries[rl.firstIdx]
}

func (rl *RaftLog) GetLast() *Entry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return &rl.entries[rl.lastIdx]
}

func (rl *RaftLog) LogItemCount() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.lastIdx - rl.firstIdx + 1
}

func (rl *RaftLog) GetEntry(idx int) *Entry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return &rl.entries[idx]
}

// GetRange
// get range log from storage engine, and return the copy
// [lo, hi]
func (rl *RaftLog) GetRange(lo, hi int) []*Entry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	ents := []*Entry{}
	for i := lo; i <= hi; i++ {
		ents = append(ents, &rl.entries[i])
	}
	return ents
}

func (rl *RaftLog) Append(newEnt *Entry) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.entries = append(rl.entries, *newEnt)
	rl.lastIdx += 1
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// todo how to impl
	// 应该持续从中读取 持久化到状态机
	applyCh        chan *ApplyMsg // apply 协程通道
	applyCond      *sync.Cond     // apply 流程控制信号量
	replicatorCond []*sync.Cond   // 复制操作控制信号量
	role           NodeRole       // 节点当前的状态
	curTerm        int            // 当前任期
	votedFor       int            // 为谁投票
	grantedVotes   int            // 已经获得票数
	logs           *RaftLog       // 日志信息
	commitIdx      int            // 已经提交的最大日志id
	lastApplied    int            // 已经apply的最大日志id
	nextIdx        []int          // 到其他节点下一个匹配的日志id信息
	matchIdx       []int          // 到其他节点当前匹配的日志id信息

	leaderId         int         // 集群当前leader节点的id
	electionTimer    *time.Timer // 选举超时定时器
	heartbeatTimer   *time.Timer // 心跳超时定时器
	heartBeatTimeout int64       // 心跳超时时间
	baseElecTimeout  int64       // 选举超时时间

	isSnapshotting bool // 是否正在打快照

}

type NodeRole uint8

const (
	NodeRoleFollower NodeRole = iota
	NodeRoleCandidate
	NodeRoleLeader
)

func NodeToString(role NodeRole) string {
	switch role {
	case NodeRoleCandidate:
		return "Candidate"
	case NodeRoleLeader:
		return "Leader"
	case NodeRoleFollower:
		return "Follower"
	}
	return "Unknow"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
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
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*Entry
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // 理解是为了快速恢复 带回来的index
	ConflictTerm  int
}

// snapshot
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 什么时候需要打快照
// 日志条目多了 需要打快照

// take a snapshot
//func (rf *Raft) Snapshot(index int, snapshot []byte) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.isSnapshotting = true
//	// GetFirstLogId ?
//	snapshotIndex := rf.logs.getLast().Index
//	if index <= snapshotIndex {
//		rf.isSnapshotting = false
//		PrintDebugLog("reject snapshot, current snapshotIndex is larger")
//		return
//	}
//	// 从当前索引到上次snapshot的索引，求的是长度
//	rf.logs.EraseBeforeWithDel(index - snapshotIndex)
//	rf.logs.SetEntFirstData([]byte{}) // 第一个操作日志号设置为空
//	PrintDebugLog(fmt.Sprintf("del log entry before idx %d", index))
//	rf.isSnapshotting = false
//	rf.logs.PersisSnapshot(snapshot)
//}

// snapshot RPC handler
// 复制时判断peer prevLogIndex 如果比当前日志的第一条索引号 还小，说明Leader已经把这条日志打到快照中了，
// 构造InstallSnapshotRequest 抵用 SnapShot RPC把快照数据发送给Follower，收到成功请求后，更新rf.matchIdx, rf.nextId
// 为 LastIncludedIndex和LastIncludedIndex+1 更新到Follower 节点复制进度

// 决定何时发送快照
//func (rf *Raft) SnapshotFunc(peerPrevLogIndex int, peerIndex int) {
//	if peerPrevLogIndex < rf.logs.GetFirst().Index {
//		firstLog := rf.logs.GetFirst()
//		snapShotArgs := InstallSnapshotArgs{
//			Term:              rf.curTerm,
//			LeaderId:          rf.me,
//			LastIncludedIndex: firstLog.Index,
//			LastIncludedTerm:  firstLog.Term,
//			Data:              rf.ReadSnapShot(),
//		}
//
//		// 解锁读锁
//		rf.mu.RUnlock()
//		PrintDebugLog(fmt.Sprintf("send snapshot to %d with %s\n", peerIndex, snapShotArgs))
//
//		reply := &InstallSnapshotReply{}
//		ok := rf.sendInstallSnapshot(peerIndex, snapShotArgs, reply)
//		if !ok {
//			PrintDebugLog(fmt.Sprintf("send snapshot to %d failed %v\n", peerIndex, snapShotArgs))
//		}
//
//		rf.mu.Lock()
//		PrintDebugLog(fmt.Sprintf("send snapshot to %d with resp %v\n", peerIndex, reply))
//
//		if reply != nil {
//			if rf.role == NodeRoleLeader && rf.curTerm == snapShotArgs.Term {
//				if snapShotArgs.Term > rf.curTerm {
//					// ToFollower
//					rf.SwitchRaftNodeRole(NodeRoleFollower)
//					rf.curTerm, rf.votedFor = reply.Term, -1
//					rf.persist()
//				} else {
//					PrintDebugLog(fmt.Sprintf("set peer %d matchIdx %d\n", peerIndex, snapShotArgs.LastIncludedIndex))
//					rf.matchIdx[peerIndex] = snapShotArgs.LastIncludedIndex
//					rf.nextIdx[peerIndex] = snapShotArgs.LastIncludedIndex
//				}
//			}
//		}
//		rf.mu.Unlock()
//	}
//})

// 处理快照请求
// 构造ApplyMsg 写到rf.applyCh
// 负责日志Apply的Goruntine调用CondInstallSnapshot安装
// restoreSnapshot将快照data数据解析，写入状态机
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		return
	}
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.SwitchRaftNodeRole(NodeRoleFollower)
	rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.baseElecTimeout+(rand.Int63()%150)))

	if args.LastIncludedIndex <= rf.commitIdx {
		return
	}

	go func() {
		rf.applyCh <- &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}()

}

// 推进follower commit位置
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	// 最大推荐到末尾日志
	newCommitIndex := Min(leaderCommit, rf.logs.GetLast().Index)
	if newCommitIndex > rf.commitIdx {
		PrintDebugLog(fmt.Sprintf("peer %d advance commit index %d at term %d", rf.me, rf.commitIdx, rf.curTerm))
		rf.commitIdx = newCommitIndex
		// 将apply推进到commitIdx
		rf.applyCond.Signal()
	}
}

// AppendEntries RPC handler
// success 只标记日志是否Append 成功
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		reply.Success = false
	}

	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = -1
	}

	rf.SwitchRaftNodeRole(NodeRoleFollower)
	rf.leaderId = args.LeaderId
	rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.baseElecTimeout+(rand.Int63()%150)))

	// 接收到一个已经打快照的位置
	if args.PrevLogTerm < rf.logs.GetFirst().Index {
		reply.Term = 0
		reply.Success = false
		PrintDebugLog(fmt.Sprintf("peer %d reject append entires request from %d\n", rf.me, args.LeaderId))
		return
	}

	// 如果日志位置不匹配
	if !rf.MatchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term = rf.curTerm
		reply.Success = false
		lastIndex := rf.logs.GetLast().Index
		// 接不上
		// log: 0,1,2,3   arg: 6,7
		if lastIndex < args.PrevLogIndex {
			PrintDebugLog(fmt.Sprintf("log conflict with term %d, index %d", -1, lastIndex+1))
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
			// 有重叠的部分
			// log 0,1,2,3  args 1,2,3
			// 重新复制整个冲突的term
		} else {
			firstIndex := rf.logs.GetFirst().Index
			reply.ConflictTerm = rf.logs.GetEntry(args.PrevLogIndex).Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs.GetEntry(index).Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}
	firstIndex := rf.logs.GetFirst().Index
	for index, entry := range args.Entries {
		// 从前向后找到第一个不一致的位置，清楚后面的内容，全部插入
		if entry.Index-firstIndex >= rf.logs.LogItemCount() || rf.logs.GetEntry(entry.Index).Term != entry.Term {
			rf.logs.EraseAfter(entry.Index-firstIndex, true)
			for _, newEnt := range args.Entries[index:] {
				rf.logs.Append(newEnt)
			}
			break
		}
	}
	// 携带Leader已经Commit到哪个位置了
	rf.advanceCommitIndexForFollower(args.LeaderCommit)
	reply.Success = true
	reply.Term = rf.curTerm

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// s1 不赞同选举
	// 1.请求任期小于当前任期
	// 2.任期相同，但已经投过票 并且不是投给当前候选人
	if args.Term < rf.curTerm || (args.Term == rf.curTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// 请求投票 无效
		reply.Term, reply.VoteGranted = rf.curTerm, false
		return
	}

	// s2 必要时切换角色
	if args.Term > rf.curTerm {
		// 转换为Follower
		rf.SwitchRaftNodeRole(NodeRoleFollower)
		rf.curTerm, rf.votedFor = args.Term, -1
	}

	lastLog := rf.logs.GetLast()

	// 保证只有最新数据的节点可以成为Leader
	// 不赞同选举
	if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
		reply.Term, reply.VoteGranted = rf.curTerm, false
		return
	}

	// 赞同选举
	rf.votedFor = args.CandidateId

	// s3 重置选举超时时间
	// 可以理解相当于收到了心跳
	rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.baseElecTimeout*(rand.Int63()%150)))
	reply.Term, reply.VoteGranted = rf.curTerm, true

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

// send append entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 增加选票
// 线程不安全的
func (rf *Raft) IncrGrantedVotes() {
	rf.grantedVotes++
}

// 切换角色
// 线程不安全的
// 似乎是原子操作
func (rf *Raft) SwitchRaftNodeRole(role NodeRole) {
	if rf.role == role {
		return
	}
	rf.role = role
	// 后续是一些状态清理操作
	PrintDebugLog(fmt.Sprintf("node change role to -> %s \n", NodeToString(role)))
	switch role {
	// 关闭心跳发送
	// 重置选举超时计时器
	case NodeRoleFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.baseElecTimeout+(rand.Int63()%150)))

	case NodeRoleCandidate:
	case NodeRoleLeader:
		// become leader, set replica (matchIdx and nextIdx) process table
		// 初始化matchIdx和next数组
		// matchIdx初始化为0 next初始化为日志索引的下一位
		lastLog := rf.logs.GetLast()
		rf.leaderId = rf.me
		for i, _ := range rf.peers {
			rf.matchIdx[i], rf.nextIdx[i] = 0, lastLog.Index
		}
		// 关闭选举计时器
		// 启用心跳超时计时器
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartBeatTimeout))
	}
}

// 线程不安全
// 但不存在竞态
func (rf *Raft) IncrCurrentTerm() {
	rf.curTerm += 1
}

// 广播心跳
func (rf *Raft) BroadcastHeartbeat() {
	// for each peers send heartbeat
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		PrintDebugLog(fmt.Sprintf("send hearbear from %d to %d", rf.me, idx))
		go func(idx int) {
			rf.replicateOneRound(idx)
		}(idx)
	}
}

/*
*
启动Replicator 将本地的更新广播给follower
*/
func (rf *Raft) BroadcastAppend() {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.replicatorCond[idx].Signal()
	}

}

/*
*
向idx Follower发送日志 直到 rf.matchIdx[idx] == rf.logs.getLast

next数组代表主节点要发送的下一个位置

match表示从节点已经接收到的位置

可能存在一种情况next更新后 发送 但并没有成功，因此match没有更新
*/
func (rf *Raft) replicateOneRound(idx int) {
	rf.mu.RLock()
	// 获取到锁的时候 可能身份已经变了
	if rf.role != NodeRoleLeader {
		rf.mu.RUnlock()
		return
	}

	// 为什么用next 不用match
	// next代表的是下一个需要发送的位置
	// match代表的是已经成功的位置
	preLogIndex := rf.nextIdx[idx] - 1
	PrintDebugLog(fmt.Sprintf("leader pre log index %d", preLogIndex))
	// 所需要的数据已经打了快照了

	if preLogIndex < rf.logs.GetFirst().Index {

		firstLog := rf.logs.GetFirst()
		// todo send kv leveldb snapshot
		snapShotArgs := &InstallSnapshotArgs{
			Term:              rf.curTerm,
			LeaderId:          rf.leaderId,
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  firstLog.Term,
		}

		rf.mu.RUnlock()
		PrintDebugLog(fmt.Sprintf("install snapshot args %v", snapShotArgs))

		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(idx, snapShotArgs, reply)
		if !ok {
			PrintDebugLog(fmt.Sprintf("send snapshot to %d failed", idx))
		}

		rf.mu.Lock()
		PrintDebugLog(fmt.Sprintf("send snapshot to %d with resp %v", idx, reply))

		if reply != nil && rf.role == NodeRoleLeader && rf.curTerm == snapShotArgs.Term && snapShotArgs.Term > rf.curTerm {
			rf.SwitchRaftNodeRole(NodeRoleFollower)
			rf.curTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}
		PrintDebugLog(fmt.Sprintf("set peer %d matchIdx %d", idx, snapShotArgs.LastIncludedIndex))
		rf.matchIdx[idx] = snapShotArgs.LastIncludedIndex
		rf.nextIdx[idx] = snapShotArgs.LastIncludedIndex + 1
		rf.mu.Unlock()
		return
	}

	firstIndex := rf.logs.GetFirst().Index
	PrintDebugLog(fmt.Sprintf("first log index %d", firstIndex))
	newEnts, _ := rf.logs.EraseBefore(preLogIndex+1, false)
	entries := make([]*Entry, len(newEnts))

	copy(entries, newEnts)

	appendEntriesArgs := &AppendEntriesArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.logs.GetEntry(preLogIndex).Term,
		Entries:      entries,
		LeaderCommit: rf.commitIdx,
	}
	rf.mu.RUnlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, appendEntriesArgs, reply)

	if !ok {
		PrintDebugLog(fmt.Sprintf("send AppendEntries to %d failed \n", idx))
	}

	// 注意appendEntriesArgs.Term 校验在这个过程中未发生过切换
	if rf.role == NodeRoleLeader && rf.curTerm == appendEntriesArgs.Term && reply != nil && reply.Success {
		// deal with appendRnt resp
		// 成功发送日志，修改next和match的位置
		PrintDebugLog(fmt.Sprintf("send AppendEntries to %d success \n", idx))
		rf.matchIdx[idx] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)
		rf.nextIdx[idx] = rf.matchIdx[idx] + 1
		rf.advanceCommitIndexForLeader()
		return
	}

	// conflict

	// 有了新leader
	if reply.Term > rf.curTerm {
		rf.SwitchRaftNodeRole(NodeRoleFollower)
		rf.curTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	// todo 需要理解一下
	if reply.Term == rf.curTerm {
		rf.nextIdx[idx] = reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			for i := appendEntriesArgs.PrevLogIndex; i >= firstIndex; i-- {
				if rf.logs.GetEntry(i).Term == reply.ConflictTerm {
					rf.nextIdx[idx] = i + 1
					break
				}
			}
		}
	}

}

// Replicator manager duplicate run
func (rf *Raft) Replicator(idx int) {
	rf.replicatorCond[idx].L.Lock()
	defer rf.replicatorCond[idx].L.Unlock()
	for !rf.killed() {
		PrintDebugLog(fmt.Sprintf("peer id: %d  wait for replicating...", idx))
		// 不是leader 或者日志已经复制完成 则等待
		for !(rf.role == NodeRoleLeader && rf.matchIdx[idx] < rf.logs.GetLast().Index) {
			rf.replicatorCond[idx].Wait()
		}
		// 将日志打包发给Follower
		// 直到matchIdx[idx] == rf.logs.getLast().Index
		rf.replicateOneRound(idx)
	}
}

// 选举逻辑
// Election make a new election
func (rf *Raft) Election() {
	fmt.Printf("%d start election", rf.me)
	rf.IncrGrantedVotes()
	rf.votedFor = rf.me
	// 选举时携带上一个log的Index和上一个log的term
	// 为了选出拥有最新数据的leader
	voteReq := &RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.GetLast().Index,
		LastLogTerm:  rf.logs.GetLast().Term,
	}
	// todo impl
	rf.persist()
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// 异步线程请求
		go func(idx int) {
			PrintDebugLog(fmt.Sprintf("send request vote to %d %v\n", idx, voteReq))

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, voteReq, reply)
			if !ok {
				PrintDebugLog(fmt.Sprintf("send request vote to %d failed", idx))
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// rf.curTerm == voteReq.Term 保证在这期间未发生其他事情导致任期变化
			// 依然为候选人
			if reply != nil && rf.curTerm == voteReq.Term && rf.role == NodeRoleCandidate {
				PrintDebugLog(fmt.Sprintf("%d send request vote， recive -> %d, curterm %d, req term %d", rf.me, idx, rf.curTerm, reply.Term))
				// 1 响应了投票 处理选举情况
				if reply.VoteGranted {
					// success granted the votes
					PrintDebugLog(fmt.Sprintf("%d grant vote from %d", rf.me, idx))
					rf.IncrGrantedVotes()
					if rf.grantedVotes > len(rf.peers)/2 {
						PrintDebugLog(fmt.Sprintf("node %d get majorite votes int term %d", rf.me, rf.curTerm))
						rf.SwitchRaftNodeRole(NodeRoleLeader)
						// 广播
						rf.BroadcastHeartbeat()
						// 重置票数
						rf.grantedVotes = 0
					}
					// todo 是否需要persist
					// 2 没有响应投票，并且已经有更大的任期转换角色
				} else if reply.Term > rf.curTerm {
					// request vote reject
					rf.SwitchRaftNodeRole(NodeRoleFollower)
					rf.curTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				}
			}
		}(idx)
	}

}

// 客户端发起Append Log
// rf级别的外部已经加过了
func (rf *Raft) Append(data []byte) *Entry {

	lastLog := rf.logs.GetLast()
	newLog := &Entry{
		Term:  rf.curTerm,
		Data:  data,
		Index: lastLog.Index + 1,
	}
	rf.logs.Append(newLog)
	rf.matchIdx[rf.me] = newLog.Index
	rf.nextIdx[rf.me] = newLog.Index + 1
	rf.persist()
	return newLog
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

	// Your code here (3B).
	//
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == NodeRoleLeader {
		return -1, -1, false
	}
	// Snapshoting reject?
	// todo why
	if rf.isSnapshotting {
		return -1, -1, false
	}

	// append log
	newLog := rf.Append(command.([]byte))

	// broadcastAppend
	rf.BroadcastAppend()

	return newLog.Index, newLog.Term, true
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

/*
*
electionTimer定时器超时后开启选举
s1 切换角色
s2 增加任期号
s3 开始选举
s4 重置选举时间
*/
func (rf *Raft) Ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			{
				// 转换为candidate
				rf.SwitchRaftNodeRole(NodeRoleCandidate)
				// 增加任期号
				rf.IncrCurrentTerm()
				// 开始选举
				rf.Election()
				// 重置选举超时时间
				rf.electionTimer.Reset(time.Millisecond * time.Duration(rf.baseElecTimeout+(rand.Int63()%150)))
			}
			// 用于发送心跳
		case <-rf.heartbeatTimer.C:
			{
				// 只有Leader需要发送心跳
				if rf.role == NodeRoleLeader {
					rf.BroadcastHeartbeat()
					rf.heartbeatTimer.Reset(time.Millisecond * time.Duration(rf.heartBeatTimeout))
				}
			}

		}
	}
}

// 检查对应位置的任期是否相符
func (rf *Raft) MatchLog(term, index int) bool {
	return index <= rf.logs.GetLast().Index && rf.logs.GetEntry(index).Term == term
}

// 每次发送给Follower都调用
// 日志提交后由leader调用
// 找出中位数索引，即已经被复制的commitIdx
// todo 没有加锁 为什么没有影响
func (rf *Raft) advanceCommitIndexForLeader() {
	// 为什么要排序
	// todo impl sort
	sort.Ints(rf.matchIdx)
	n := len(rf.matchIdx)
	// [18 18 '19 19 20] majority replicate log index 19
	// [18 '18 19] majority replicate log index 18
	// [18 '18 19 20] majority replicate log index 18

	// 表示已经被大多数写入的位置
	// todo 是否需要保存matchIdx
	newCommitIndex := rf.matchIdx[n-(n/2+1)]
	if newCommitIndex > rf.commitIdx {
		// 检查了任期是否被替换
		if rf.MatchLog(rf.curTerm, newCommitIndex) {
			PrintDebugLog(fmt.Sprintf("peer %d advance commit index %d at term %d", rf.me, rf.commitIdx, rf.curTerm))
			// 将commitIdx 推进到 newCommitIndex
			rf.commitIdx = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// Applier() Write the commited message to the applyCh channel
// and update lastApplied
func (rf *Raft) Applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIdx {
			PrintDebugLog("applier ...")
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIdx, rf.lastApplied
		entries := make([]*Entry, commitIndex-lastApplied)
		copy(entries, rf.logs.GetRange(lastApplied, commitIndex+1))
		rf.mu.Unlock()
		PrintDebugLog(fmt.Sprintf("%d, applies entries %d-%d in term %d", rf.me, rf.lastApplied, commitIndex, rf.curTerm))

		// 通过channel 发送，这里通过rpc实现
		for _, entry := range entries {
			rf.applyCh <- &ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		// 会不会有风险，如果有人修改了commitIdx
		rf.lastApplied = Max(rf.lastApplied, rf.commitIdx)
		rf.mu.Unlock()
	}
}

func PrintDebugLog(message string) {
	// 获取当前时间
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	// 打印日志到控制台
	log.Printf("[%s] DEBUG: %s\n", currentTime, message)
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

	rf.role = NodeRoleFollower

	// Your initialization code here (3A, 3B, 3C).
	// 初始化心跳超时时间 50ms 一般为选举超时时间的 1/3
	rf.heartBeatTimeout = 50
	//rf.heartbeatTimer = time.NewTimer(time.Millisecond * time.Duration(rf.heartBeatTimeout))
	// 初始化选举超时时间 150~300ms
	rf.baseElecTimeout = 150
	rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rf.baseElecTimeout+rand.Int63()%150))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Ticker()

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.Replicator(idx)
	}

	go rf.Applier()
	return rf
}
