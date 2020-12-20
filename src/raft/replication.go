package raft

/*==========================================
	Log Entry 日志相关函数定义
==========================================*/

func (rf *Raft) updateCommitIndex() {
	// 统计是否majority matched
	n := len(rf.peers)
	for index := rf.commitIndex + 1; index <= len(rf.log); index++ {
		counter := 0
		for _, matched := range rf.matchIndex {
			if matched >= index {
				counter++
			}
			if counter > n/2 {
				rf.commitIndex = index
				break
			}
		}
		if rf.commitIndex != index {
			break
		}
	}
}

func (rf *Raft) getAppendLogs(index int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	// TODO: to be filled
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	if lastLogIndex >= rf.nextIndex[index] {
		prevLogIndex = rf.nextIndex[index] - 1
		prevLogTerm = rf.log[prevLogIndex].Term
		res := append([]LogEntry{}, rf.log[rf.nextIndex[index]:]...)
		return prevLogIndex, prevLogTerm, res
	}

	return 0, 0, nil
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

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		//DPrintf("[DEBUG] Svr[%v]:(%s, Term:%v) Get Heart Beats from %v", rf.me, rf.getRole(), rf.currentTerm, args.LeaderId)
	} else {
		DPrintf("[DEBUG] Svr[%v]:(%s, Term:%v) Start Func AppendEntries with args:%+v", rf.me, rf.getRole(), rf.currentTerm, args)
		defer DPrintf("[DEBUG] Svr[%v]:(%s) End Func AppendEntries with args:%+v, reply:%+v", rf.me, rf.getRole(), args, reply)
	}

	// 初始化
	reply.Success = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		// reply false if term < currentTerm
		DPrintf("[DEBUG] Svr[%v]:(%s) Reject AppendEntries due to currentTerm > args.Term", rf.me, rf.getRole())
		return
	}

	rf.currentTerm = args.Term
	rf.switchToFollower(args.Term)
	rf.resetElectionTimer() // 收到了有效的Leader的消息，重置选举的定时器

	if len(args.Entries) == 0 {
		// heartbeats
		return
	}

	// 考虑rf.log[args.PrevLogIndex]有没有内容
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	if lastLogIndex < args.PrevLogIndex {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		// 没有prevLogIndex的entry也是不包含
		// TODO: 是否会出现空洞的情况
		DPrintf("[DEBUG] Svr[%v]:(%s) Reject AppendEntries due to lastLogIndex < args.PrevLogIndex", rf.me, rf.getRole())
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		DPrintf("[DEBUG] Svr[%v]:(%s) Reject AppendEntries due to rf.log[args.PrevLogIndex].Term != args.PrevLogTerm", rf.me, rf.getRole())
		return
	}

	nextIndex := args.PrevLogIndex + 1
	DPrintf("[DEBUG] Svr[%v]:(%s) nextIndex is %v, lastLogIndex is %v", rf.me, rf.getRole(), nextIndex, lastLogIndex)
	if lastLogIndex >= nextIndex &&
		rf.log[nextIndex].Term != args.Entries[0].Term {
		// conflict ,delete the existing entry and all follow it
		DPrintf("[DEBUG] Svr[%v]:(%s) Delete existing entry and all follow it", rf.me, rf.getRole())
		rf.log = rf.log[0:nextIndex] // FIXME: 左取右不取
	}

	reply.Success = true
	rf.log = append(rf.log, args.Entries...)

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		DPrintf("[DEBUG] Svr[%v]:(%s) Follower Update commitIndex", rf.me, rf.getRole())
		lastLogIndex, _ := rf.getLastLogIndexTerm()
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPCToPeer(slave int) {
	rf.mu.Lock()
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
			rf.currentTerm = reply.Term
			rf.switchToFollower(reply.Term)
			rf.resetElectionTimer()
		} else {
			if len(args.Entries) > 0 {
				DPrintf("[DEBUG] Svr[%v] (%s) Get reply for AppendEntries from %v, reply.Term <= rf.currentTerm, reply is %+v", rf.me, rf.getRole(), slave, reply)
				if rf.role == LEADER && reply.Success {
					rf.matchIndex[slave] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[slave] = rf.matchIndex[slave] + 1
					DPrintf("[DEBUG] Svr[%v] (%s): matchIndex[%v] is %v", rf.me, rf.getRole(), slave, rf.matchIndex[slave])
					rf.updateCommitIndex()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesRPCToOthers() {
	//DPrintf("[DEBUG] Svr[%v]:(%s) sendAppendEntriesRPCToOthers", rf.me, rf.getRole())
	for slave := range rf.peers {
		// 不发送给自己
		if slave == rf.me {
			continue
		} else {
			go rf.sendAppendEntriesRPCToPeer(slave)
		}
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
		rf.sendAppendEntriesRPCToOthers()
	}
}
