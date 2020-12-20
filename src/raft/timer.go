package raft

import (
	"math/rand"
	"time"
)

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
