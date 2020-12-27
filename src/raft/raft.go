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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"sync"
)
import "labrpc"
import "time"

// import "bytes"
// import "encoding/gob"

// To heartbeats, need to define AppendEntries struct
// Also need to implement AppendEntries RPC handler
// Make sure the election timeouts DON'T always fire at the same time
/*==========================================
	Const 常量定义
==========================================*/

// Raft中Server的三种状态
const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

// 时钟相关的
const (
	ElectionTimeoutMin = 100
	ElectionTimeoutMax = 500
	HeartBeatsInterval = 50
)

// AppendLog失败后是否需要回退
const (
	BackOff = -100
)

/*==========================================
	Struct 结构体定义
==========================================*/

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

type LogEntry struct {
	Command interface{} // 状态机的命令
	Term    int         // log entry的term
	Index   int         // log entry的index
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int        // server已知的最新term，初始化为0，单调递增
	votedFor    int        // 当前term中所投票的id，如果没有投票，则为null
	log         []LogEntry // TODO: First index is 1

	// Volatile state on all servers
	commitIndex int // committed的最大的log entry index，初始化为0，单调递增
	lastApplied int // 应用到状态机的最大的log entry index，初始化为0，单调递增

	// Volatile state on leaders
	// TODO Reinitialized after election
	nextIndex  []int // To send, 对每个server，下一个要发送的log entry的序号， 初始化为 leader last log index+1 TODO: 初始化
	matchIndex []int // To replicated，对每个server，已知的最高的已经复制成功的序号

	// Self defined
	role           int         // 服务器状态
	leaderID       int         // Follower的Leader
	electionTimer  *time.Timer // Leader Election的定时器 FIXME: GUIDE SAYS DO NOT USE TIMER
	heartBeatTimer *time.Timer // Heart Beat的定时器
	applyCh        chan ApplyMsg
}

type AppendEntriesArgs struct {
	Term         int        // 领导者的term
	LeaderId     int        // 领导者的ID，
	PrevLogIndex int        // 在append新log entry前的log index
	PrevLogTerm  int        // 在append新log entry前的log index下的term
	Entries      []LogEntry // 要append log entries
	LeaderCommit int        // 领导者的commitIndex

}

type AppendEntriesReply struct {
	// TODO:
	Term      int  //
	Success   bool // 成功，if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int  // 下一个要append的Index，根据AppendEntries的情况来判断
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	// TODO:
	Term         int // 候选人的term
	CandidatedId int // 候选人的ID
	LastLogIndex int // 候选人日志中最后一条的序号
	LastLogTerm  int // 候选人日志中最后一条的term
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	// TODO:
	Term        int  // 当前的term，用于使候选人更新状态
	VoteGranted bool // 若为真，则表示候选人接受了投票
}

/*==========================================
	时钟相关函数定义
==========================================*/

func randomElectionTimeout() time.Duration {
	// 选举超时时间，100~500ms
	x := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	n := time.Duration(x)
	timeout := n * time.Millisecond
	return timeout
}

func (rf *Raft) resetElectionTimer() {
	duration := randomElectionTimeout()
	rf.electionTimer.Reset(duration)
	//DPrintf("[DEBUG] Svr[%v]:(%s) Reset ElectionTimer with %v\n", rf.me, rf.getRole(), duration)
}

func getHeartBeatInterval() time.Duration {
	return HeartBeatsInterval * time.Millisecond
}

func (rf *Raft) resetHeartBeatTimer() {
	duration := getHeartBeatInterval()
	rf.heartBeatTimer.Reset(duration)
	//DPrintf("[DEBUG] Svr[%v]:(%s) Reset HeartBeatTimer with %v\n", rf.me, rf.getRole(), duration)
}

/*==========================================
	Leader Election 选举函数定义
==========================================*/

//
// example RequestVote RPC handler.
//

func (rf *Raft) switchToLeader() {
	rf.role = LEADER
	rf.leaderID = rf.me
	rf.resetElectionTimer()
	rf.heartBeatTimer.Reset(0) // 马上启动心跳机制

	// matchIndex[] 跟踪 Leader 和每个 Follower 匹配到的日志条目
	// nextIndex[] 保存要发送每个 Follower 的下一个日志条目
	n := len(rf.peers)
	nlog := len(rf.log) // 0 | 1, 2, 3

	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)

	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = nlog
	}
	rf.matchIndex[rf.me] = nlog - 1
	rf.persist()
	DPrintf("[DEBUG] Svr[%v]:(%s) switch to leader and reset heartBeatTimer 0.", rf.me, rf.getRole())
}

func (rf *Raft) switchToCandidate() {
	rf.currentTerm++
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("[DEBUG] Svr[%v]:(%s) switch to candidate.", rf.me, rf.getRole())

}

func (rf *Raft) switchToFollower(term int) {
	var flag bool
	if rf.role != FOLLOWER {
		flag = true
	}
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	//rf.resetElectionTimer()
	if flag {
		DPrintf("[DEBUG] Svr[%v]:(%s) switch to follower.", rf.me, rf.getRole())
	}
	rf.persist()
}

func (rf *Raft) getLastLogIndexTerm() (lastLogIndex int, lastLogTerm int) {
	last := len(rf.log) - 1
	lastLogIndex = rf.log[last].Index
	lastLogTerm = rf.log[last].Term
	assert(last, lastLogIndex, fmt.Sprintf("Server[%v](%s) %+v, The slice index should be equal to lastLogIndex", rf.me, rf.getRole(), rf.log))
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatedId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

func (rf *Raft) sendRequestVoteRPCToOthers() {
	DPrintf("[DEBUG] Svr[%v]:(%s) Begin sendRequestVoteRPCToOthers", rf.me, rf.getRole())
	n := len(rf.peers)
	voteCh := make(chan bool, n) // 接收来自各个节点的reply

	for server := range rf.peers {
		// 不发送给自己
		if server == rf.me {
			continue
		} else {
			args := rf.getRequestVoteArgs()
			// 开启新go routine，分别发送RequestVote给对应的server
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					voteCh <- reply.VoteGranted
				} else {
					voteCh <- false // 如果没有收到回复，视为不投票
				}

			}(server)
		}
	}

	// 统计投票结果
	replyCounter := 1 // 收到的回复
	validCounter := 1 // 投自己的回复
	for {
		vote := <-voteCh
		rf.mu.Lock()
		if rf.role == FOLLOWER {
			rf.mu.Unlock()
			DPrintf("[DEBUG] Svr[%v]:(%s) has been a follower", rf.me, rf.getRole())
			return
		}
		rf.mu.Unlock()
		replyCounter++
		if vote == true {
			validCounter++
		}
		if replyCounter == n || // 所有人都投票了
			validCounter > n/2 || // 已经得到了majority投票
			replyCounter-validCounter > n/2 { // 已经有majority的投票人不是投自己
			break
		}
	}

	if validCounter > n/2 {
		// 得到了majority投票，成为Leader
		rf.mu.Lock()
		rf.switchToLeader()
		rf.mu.Unlock()
	} else {
		DPrintf("[DEBUG] Svr[%v]:(%s) get %v vote, Fails", rf.me, rf.getRole(), validCounter)
	}

}

func (rf *Raft) LeaderElectionLoop() {
	for {
		// 等待 election timeout
		<-rf.electionTimer.C // 表达式会被Block直到超时
		rf.resetElectionTimer()

		rf.mu.Lock()
		DPrintf("[DEBUG] Svr[%v]:(%s) Start Leader Election Loop", rf.me, rf.getRole())
		if rf.role == LEADER {
			DPrintf("[DEBUG] Svr[%v]:(%s) End Leader Election Loop, Leader is Svr[%v]", rf.me, rf.getRole(), rf.me)
			rf.mu.Unlock()
			continue
		}

		if rf.role == FOLLOWER || rf.role == CANDIDATE {
			rf.switchToCandidate()
			rf.mu.Unlock()
			rf.sendRequestVoteRPCToOthers()
		}
	}
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// TODO: Implement it so that servers will vote for one another
	rf.mu.Lock()
	DPrintf("[DEBUG] Svr[%v]:(%s, Term:%v) Start Func RequestVote with args:%+v", rf.me, rf.getRole(), rf.currentTerm, args)
	defer rf.mu.Unlock()
	defer DPrintf("[DEBUG] Svr[%v]:(%s) End Func RequestVote with args:%+v, reply:%+v", rf.me, rf.getRole(), args, reply)

	// 初始化
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm < args.Term {
		rf.switchToFollower(args.Term)
	}

	// 是否可以进行投票
	if rf.rejectVote(args) {
		return
	}

	if rf.votedFor == args.CandidatedId {
		reply.VoteGranted = true
		rf.resetElectionTimer()
		return
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}

	DPrintf("[DEBUG] Svr[%v]:(%s) Vote for %v", rf.me, rf.getRole(), args.CandidatedId)
}

func (rf *Raft) rejectVote(args RequestVoteArgs) bool {
	if rf.role == LEADER {
		return true
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidatedId {
		return true
	}
	// $5.4.1的限制
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	if lastLogTerm != args.LastLogTerm {
		return lastLogTerm > args.LastLogTerm
	}
	return lastLogIndex > args.LastLogIndex
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/*==========================================
	Log Entry 日志相关函数定义
==========================================*/

func getMajoritySameIndex(matchIndex []int) int {
	n := len(matchIndex)
	tmp := make([]int, n)
	copy(tmp, matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	return tmp[n/2]
}

func (rf *Raft) getAppendLogs(slave int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {

	nextIndex := rf.nextIndex[slave]
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()

	if nextIndex <= 0 || nextIndex > lastLogIndex {
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	entries = append([]LogEntry{}, rf.log[nextIndex:]...)
	prevLogIndex = nextIndex - 1
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	return
}

func (rf *Raft) getAppendEntriesArgs(slave int) AppendEntriesArgs {
	prevLogIndex, preLogTerm, entries := rf.getAppendLogs(slave)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) getNextIndex() int {
	// append log entry后必须再调用一次否则会返回错误的结果
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	nextIndex := lastLogIndex + 1
	return nextIndex
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO:
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// 初始化
	reply.Success = false
	reply.Term = rf.currentTerm

	// 拒绝Term小于自己的节点的Append请求
	if rf.currentTerm > args.Term {
		// reply false if term < currentTerm
		DPrintf("[DEBUG] Svr[%v]:(%s) Reject AppendEntries due to currentTerm > args.Term", rf.me, rf.getRole())
		return
	}

	// 判断是否是来自leader的心跳
	if len(args.Entries) == 0 {
		DPrintf("[DEBUG] Svr[%v]:(%s, Term:%v) Get Heart Beats from %v", rf.me, rf.getRole(), rf.currentTerm, args.LeaderId)
	} else {
		DPrintf("[DEBUG] Svr[%v]:(%s, Term:%v) Start Func AppendEntries with args:%+v", rf.me, rf.getRole(), rf.currentTerm, args)
		defer DPrintf("[DEBUG] Svr[%v]:(%s) End Func AppendEntries with args:%+v, reply:%+v", rf.me, rf.getRole(), args, reply)
	}

	rf.currentTerm = args.Term
	rf.switchToFollower(args.Term)
	rf.resetElectionTimer() // 收到了有效的Leader的消息，重置选举的定时器

	// 考虑rf.log[args.PrevLogIndex]有没有内容，即上一个应该同步的位置
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	if args.PrevLogIndex > lastLogIndex {
		DPrintf("[DEBUG] Svr[%v]:(%s) Reject AppendEntries due to lastLogIndex < args.PrevLogIndex", rf.me, rf.getRole())
		reply.NextIndex = rf.getNextIndex()
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[DEBUG] Svr[%v]:(%s) Previous log entries do not match", rf.me, rf.getRole())
		reply.NextIndex = BackOff
	} else {
		reply.Success = true
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...) // [a:b]，左取右不取，如果有冲突就直接截断
	}

	if reply.Success {
		rf.leaderID = args.LeaderId
		if args.LeaderCommit > rf.commitIndex {
			lastLogIndex, _ := rf.getLastLogIndexTerm()
			rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
			DPrintf("[DEBUG] Svr[%v]:(%s) Follower Update commitIndex, lastLogIndex is %v", rf.me, rf.getRole(), lastLogIndex)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPCToPeer(slave int) {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	args := rf.getAppendEntriesArgs(slave)
	if len(args.Entries) > 0 {
		DPrintf("[DEBUG] Svr[%v]:(%s) sendAppendEntriesRPCToPeer send to Svr[%v]", rf.me, rf.getRole(), slave)
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(slave, args, &reply)
	// FIXME: 注意，这里的ok是调用成功，而不是reply的ok
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			DPrintf("[DEBUG] Svr[%v] (%s) Get reply for AppendEntries from %v, reply.Term > rf.currentTerm", rf.me, rf.getRole(), slave)
			rf.switchToFollower(reply.Term)
			rf.resetElectionTimer()
			rf.mu.Unlock()
			return
		}

		if rf.role != LEADER || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		DPrintf("[DEBUG] Svr[%v] (%s) Get reply for AppendEntries from %v, reply.Term <= rf.currentTerm, reply is %+v", rf.me, rf.getRole(), slave, reply)
		if reply.Success {
			lenEntry := len(args.Entries)
			rf.matchIndex[slave] = args.PrevLogIndex + lenEntry
			rf.nextIndex[slave] = rf.matchIndex[slave] + 1
			DPrintf("[DEBUG] Svr[%v] (%s): matchIndex[%v] is %v", rf.me, rf.getRole(), slave, rf.matchIndex[slave])
			majorityIndex := getMajoritySameIndex(rf.matchIndex)
			if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
				rf.commitIndex = majorityIndex
				DPrintf("[DEBUG] Svr[%v] (%s): Update commitIndex to %v", rf.me, rf.getRole(), rf.commitIndex)
			}
		} else {
			// 失败，要重试
			DPrintf("[DEBUG] Svr[%v] (%s): append to Svr[%v]Success is False, reply is %+v", rf.me, rf.getRole(), slave, &reply)
			if reply.NextIndex > 0 {
				rf.nextIndex[slave] = reply.NextIndex
			} else if reply.NextIndex == BackOff {
				// 直接后退一个term
				prevIndex := args.PrevLogIndex
				for prevIndex > 0 && rf.log[prevIndex].Term == args.PrevLogTerm {
					prevIndex--
				}
				rf.nextIndex[slave] = prevIndex + 1
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeatLoop() {
	for {
		<-rf.heartBeatTimer.C
		rf.resetHeartBeatTimer()

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		for slave := range rf.peers {
			// 不发送给自己
			if slave == rf.me {
				rf.nextIndex[slave] = len(rf.log) + 1
				rf.matchIndex[slave] = len(rf.log)
				continue
			} else {
				go rf.sendAppendEntriesRPCToPeer(slave)
			}
		}
	}
}

func (rf *Raft) apply(index int) {
	msg := ApplyMsg{
		Index:       index,
		Command:     rf.log[index].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}
	DPrintf("[DEBUG] Srv[%v](%s) apply log entry %+v", rf.me, rf.getRole(), rf.log[index].Command)
	rf.applyCh <- msg
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.lastApplied)
		}
		rf.mu.Unlock()
	}
}

/*==========================================
	Persistent 持久化相关函数定义
==========================================*/
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

/*==========================================
	其他函数定义
==========================================*/

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) logToString() string {
	res := ""
	for index, log := range rf.log {
		if index <= rf.commitIndex {
			res += fmt.Sprintf("{C*%v T*%v i*%v}", log.Command, log.Term, log.Index)
		} else {
			res += fmt.Sprintf("{C:%v T:%v i:%v}", log.Command, log.Term, log.Index)

		}
	}
	return res
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("LID: %v;Term:%d;log:%s;commitIndex:%v;",
		rf.leaderID, rf.currentTerm, rf.logToString(), rf.commitIndex)
}

func (rf *Raft) toStringWithoutLog() string {
	return fmt.Sprintf("LID: %v;Term:%d;commitIndex:%v;",
		rf.leaderID, rf.currentTerm, rf.commitIndex)
}

func (rf *Raft) getRole() string {
	var role string
	switch rf.role {
	case LEADER:
		role = "Lead"
	case FOLLOWER:
		role = "Foll"
	case CANDIDATE:
		role = "Cand"
	}
	//return role + " " + rf.toStringWithoutLog()
	return role + " " + rf.toString()
	//return role
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func assert(a interface{}, b interface{}, msg interface{}) {
	if a != b {
		panic(msg)
	}
}

/*==========================================
	Raft 运行函数定义
==========================================*/

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

	rf.mu.Lock()
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	index = lastLogIndex + 1
	term = rf.currentTerm
	isLeader = rf.role == LEADER

	if isLeader {
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.log = append(rf.log, logEntry)
		DPrintf("[Debug] Svr[%v]:(%s, Term:%v) get command %+v", rf.me, rf.getRole(), rf.currentTerm, command)
		rf.matchIndex[rf.me] = index
		rf.persist()
	}

	rf.mu.Unlock()
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
	// TODO: Modify Make() to create a background goroutine that starts an election by sending out RequestVote RPC when it hasn't heard from another peer for a while
	DPrintf("[DEBUG] Svr[%v]: Start Func Make()\n", me)
	defer DPrintf("[DEBUG] Svr[%v]: End Func Make()\n", me)
	// 初始化 Raft Server状态
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1 // 用-1表示null
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0

	// 初始化log，并加入一个空的守护日志（因为log的index从1开始）
	guideEntry := LogEntry{
		Command: nil,
		Term:    0,
		Index:   0,
	}
	rf.log = append(rf.log, guideEntry)
	rf.role = FOLLOWER
	rf.leaderID = -1
	rf.readPersist(persister.ReadRaftState())

	// 初始化选举的计时器
	rf.electionTimer = time.NewTimer(100 * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(getHeartBeatInterval())

	// Sever启动时，是follower状态。 若收到来自leader或者candidate的有效PRC，就持续保持follower状态。
	go rf.LeaderElectionLoop()
	go rf.heartBeatLoop()
	go rf.applyLoop()

	return rf
}
