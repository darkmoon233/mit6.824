package raft

import (
	"math/rand"
	"time"
)

const (
	ELECTION_TIMEOUT_MAX = 150
	ELECTION_TIMEOUT_MIN = 100
	HEARTBEAT_TIMEOUT    = 100
)

func getSleepTime(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
