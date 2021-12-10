package raft

import "time"

// 你认为我是个follower,所以你会向我发送让我投票和让我追加日志的命令,事实上我是个什么东西大家还没有达成共识
// 在raft中,每个server的身份是不确定的,没有人真的知道自己处于什么位置
// 1. leader也得看appendEntry返回回来的term和自己比较的情况,也得防着别人冷不丁的给自己发一个投票请求
// 2. candidate就更别提了,实时看着选票情况
// 3. 不过你要是个follower,你只要管好你自己就行了,谁来问你问题,你照实说就行,你完全被动,除非没人管你了,你才得想着办法看看是不是可以混个leader当当

// follower执行日志追加命令
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 追加日志规则
	// rule1 追加日志命令的选期落后于当前server,拒绝他
	// rule2 追加日志命令认定的日志位置和本server存储的位置不匹配,拒绝
	// rule3 如果追加的日志命令和自身的日志有冲突,以传入的为准

	// rule1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || rf.role == CANDIDATE {
		rf.changeRole(FOLLOWER)
		rf.currentTerm = args.Term
		rf.lastLogAppendTime = time.Now()
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

}

//RequestVote
