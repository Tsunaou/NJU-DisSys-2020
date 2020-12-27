package raft

import "fmt"

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
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.switchToFollower(args.Term)
	}

	// 是否可以进行投票
	if rf.rejectVote(args) {
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidatedId
	reply.VoteGranted = true
	rf.resetElectionTimer()
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
