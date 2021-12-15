package raft

import "time"

// 维系系统有且仅有一个leader是通过leader的心跳和失去心跳后的自动发起选举两件事共同支持的

// raft的持续运行和触发等待
func (rf *Raft) raftLoop() {
	for !rf.killed() {
		select {
		case <-rf.tickerElectionChan:
			rf.startElection()
		case <-rf.tickerHeartbeatChan:
			rf.startHeartBeat()
		}
	}
}

// 不定期发起选举检查
// 在server的睡眠到睡眠唤醒时收到过心跳/日志追加,就不需要选举,否则开始选举
// 心跳的时间应该比server的收不到心跳要发起的时间更短才合理
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		sleepAt := time.Now()
		sleepTime := getSleepTime(int64(rf.me))
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		rf.mu.Lock()

		if rf.lastLogAppendTime.Before(sleepAt) && rf.role != LEADER {
			DPrintf("[timerElection+++++++] raft %d heartbeat timeout | current term: %d | current state: %d\n", rf.me, rf.currentTerm, rf.role)
			rf.tickerElectionChan <- true
		}
		rf.mu.Unlock()
	}
}

// 定期发起心跳或者日志追加
func (rf *Raft) heartbeatTicker() {

	for !rf.killed() {
		sleepTime := HEARTBEAT_TIMEOUT * time.Millisecond
		time.Sleep(sleepTime)
		rf.mu.Lock()

		if rf.role == LEADER {
			DPrintf("[timerHeartbeatTicker+++++++] raft %d heartbeat timeout | current term: %d | current state: %d\n", rf.me, rf.currentTerm, rf.role)
			rf.tickerHeartbeatChan <- true
		}

		rf.mu.Unlock()

	}

}
