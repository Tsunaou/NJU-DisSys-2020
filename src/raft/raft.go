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
	"sync"
)
import "labrpc"
import "time"

// import "bytes"
// import "encoding/gob"

// To heartbeats, need to define AppendEntries struct
// Also need to implement AppendEntries RPC handler
// Make sure the election timeouts DON'T always fire at the same time
// TODO: Implement the leader and follower code to append new log entries
// TODO: implementing Start()
// TODO: completing the AppendEntries RPC structs, sending them, and completing the AppendEntry RPC handler
// TODO: pass the TestBasicAgree() test, try to pass all test before “Persist”
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
	HeartBeatsInterval = 100
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
	// TODO Update on stable storage before responding to RPCS
	currentTerm int        // server已知的最新term，初始化为0，单调递增
	votedFor    int        // 当前term中所投票的id，如果没有投票，则为null
	log         []LogEntry // TODO: First index is 1

	// Volatile state on all servers
	commitIndex int // committed的最大的log entry index，初始化为0，单调递增
	lastApplied int // 应用到状态机的最大的log entry index，初始化为0，单调递增

	// Volatile state on leaders
	// TODO Reinitialized after election
	nextIndex  []int // To send, 对每个server，下一个要发送的log entry的序号， 初始化为 leader last log index+1 TODO: 初始化？
	matchIndex []int // To replicated，对每个server，已知的最高的已经复制成功的序号

	// Self defined
	role           int         // 服务器状态
	leaderID       int         // Follower的Leader
	electionTimer  *time.Timer // Leader Election的定时器 FIXME: GUIDE SAYS DO NOT USE TIMER\
	heartBeatTimer *time.Timer // Heart Beat的定时器
	applyCh        chan ApplyMsg
}

type AppendEntriesArgs struct {
	// TODO:
	Term         int        // 领导者的term
	LeaderId     int        // 领导者的ID，
	PrevLogIndex int        // 在append新log entry前的log index
	PrevLogTerm  int        // 在append新log entry前的log index下的term
	Entries      []LogEntry // 要append log entries TODO: 如果是空的用来heartbeats
	LeaderCommit int        // 领导者的commitIndex

}

type AppendEntriesReply struct {
	// TODO:
	Term      int  //
	Success   bool // 成功，if follower contained entry matching prevLogIndex and prevLogTerm TODO: 最后一个匹配还是有匹配然后截断
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
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.nextIndex)
	e.Encode(rf.matchIndex)
	e.Encode(rf.role)
	e.Encode(rf.leaderID)
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
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.nextIndex)
	d.Decode(&rf.matchIndex)
	d.Decode(&rf.role)
	d.Decode(&rf.leaderID)
}

/*==========================================
	其他函数定义
==========================================*/

// FIXME: Self-Defined
// Rules for Servers
func (rf *Raft) AllServersRules() {
	// TODO:
}

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

	rf.readPersist(persister.ReadRaftState())

	// 初始化选举的计时器
	rf.electionTimer = time.NewTimer(100 * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(getHeartBeatInterval())
	// Sever启动时，是follower状态。 若收到来自leader或者candidate的有效PRC，就持续保持follower状态。
	rf.role = FOLLOWER
	rf.leaderID = -1
	rf.switchToFollower(0)

	go rf.LeaderElectionLoop()
	go rf.heartBeatLoop()
	go rf.applyLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
