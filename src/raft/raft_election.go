package raft

// 切换角色并且初始化数据
func (rf *Raft) changeRole(role int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if role == rf.role {
	//	DPrintf("[changeRole+++++++] Sever %d, term %d, current role %d, target role %d, same role ,skip", rf.me, rf.currentTerm, rf.role, role)
	//	return
	//}
	DPrintf("[changeRole+++++++] Sever %d, term %d, current role %d, target role %d", rf.me, rf.currentTerm, rf.role, role)
	switch role {
	case FOLLOWER:
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.getVoteNum = 0
	case CANDIDATE:
		rf.role = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.getVoteNum = 1
	case LEADER:
		rf.role = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0
	}
}

// 执行选举动作(不知道为什么我也写得这么长,丑死)
func (rf *Raft) startElection() {
	rf.changeRole(CANDIDATE)
	DPrintf("[JoinElection+++++++] Sever %d, term %d", rf.me, rf.currentTerm)

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			rvArgs := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()

			rvReply := RequestVoteReply{}
			DPrintf("[RequestVoteArgs+++++++] Target Server:%d, CandidateId:%d, Args Term %d", server, rvArgs.CandidateId, rvArgs.Term)

			res := rf.sendRequestVote(server, &rvArgs, &rvReply)

			DPrintf("[RequestVoteReply+++++++] Target Server:%d, VoteGranted:%t, Reply Term %d", server, rvReply.VoteGranted, rvReply.Term)

			if !res {
				return
			}
			// 分析投票结果
			// rule1: 分析选票时如果选期已经过了或者身份已经不是候选人了,就放弃分析选票
			// rule2: 选票的选期如果比候选人的选期更新,那就放弃选举,转换身份为follower
			// rule3: 获得了超过半数的选票,转换身份为leader
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// rule1
			if rf.role != CANDIDATE {
				DPrintf("[Election+++++++] Sever %d, term %d, election term %d, role change", rf.me, rf.currentTerm, rvArgs.Term)
				return
			}
			if rvArgs.Term < rf.currentTerm {
				DPrintf("[Election+++++++] Sever %d, term %d, election term %d, vote expired", rf.me, rf.currentTerm, rvArgs.Term)
				return
			}
			// rule2
			if rvReply.Term > rf.currentTerm {
				DPrintf("[Election+++++++] Sever %d, term %d, election term %d, local term out, change to follower, callback term is %d", rf.me, rf.currentTerm, rvArgs.Term, rvReply.Term)

				rf.changeRole(FOLLOWER)
				return
			}
			// rule3
			if rvReply.VoteGranted && rf.currentTerm == rvReply.Term {
				rf.getVoteNum += 1
				DPrintf("[Election+++++++] Sever %d, term %d, election term %d, vote from %d,  get one vote", rf.me, rf.currentTerm, rvArgs.Term, server)

				if rf.getVoteNum > 1/2*len(rf.peers) {
					DPrintf("[Election+++++++] Sever %d, term %d, election term %d, slection success", rf.me, rf.currentTerm, rvArgs.Term)

					rf.changeRole(LEADER)
				}
				return
			}
		}(index)
	}
}

// 执行心跳动作
func (rf *Raft) startHeartBeat() {
	DPrintf("total peers is %d", len(rf.peers))
	for index := range rf.peers {

		if rf.me == index {
			return
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}

			aeArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PreLogTerm:   rf.log[rf.nextIndex[server]-1].Term,
				Entries:      rf.log[rf.nextIndex[server]-1:],
				LeaderCommit: 0,
			}
			aeReply := AppendEntriesReply{}

			rf.mu.Unlock()

			res := rf.sendAppendEntries(server, &aeArgs, &aeReply)
			if !res {
				return
			} else {
				rf.mu.Lock()

				if rf.role != LEADER {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				// 分析心跳
				// rule1: 返回的选期比自己的选期还要高,放弃leader身份,去当个follower得了
				// // rule2: 返回的选期比自己的选期要低(不会发生,目标server的选期被纠正了,发生了证明出现了系统错误)
				// // rule2: 心跳说,append失败了,得知道是哪里失败了,更新要给他发的日志index,但是2A应该不用管
				// rule2: 心跳说,成功了,更新要给server的nextIndex记录

				// rule1
				rf.mu.Lock()
				if aeReply.Term > rf.currentTerm {
					rf.currentTerm = aeReply.Term
					rf.changeRole(FOLLOWER)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				// rule2
				if !aeReply.Success {
					return
				} else {
					rf.mu.Lock()
					rf.matchIndex[server] += len(aeArgs.Entries) + aeArgs.PrevLogIndex
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.mu.Unlock()
				}
			}

		}(index)
	}

}
