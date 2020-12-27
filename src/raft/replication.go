package raft

import (
	"sort"
	"time"
)

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
