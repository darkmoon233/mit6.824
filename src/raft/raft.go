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
	"sync"
	"time"
)
import "sync/atomic"

import "labrpc"

//import "6.824/labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// server 角色
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

// raft日志
type Entry struct {
	Command interface{} //业务相关,框架无关
	Term    int         //选期
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 论文figure2提到的变量
	currentTerm int     //当前server的选期
	votedFor    int     //当前server的投票目标
	log         []Entry //本server的log
	// 所有的commitIndex和lastApplied都是单调递增的,同时必须和leader确认了是共识的log才会被提交,根据数据
	commitIndex int   //本机已提交日志的最大下标
	lastApplied int   //应用于复制状态机的最大日志下标
	nextIndex   []int //leader专用,对所有server记录的要发给他们的下一条日志的index
	matchIndex  []int //leader专用,对所有的server记录他们已经能提供副本的位置

	//自己编的
	role                int       //本server的角色
	tickerElectionChan  chan bool //是否发起选举
	tickerHeartbeatChan chan bool // 是否发起心跳
	lastLogAppendTime   time.Time //最后或得到的leader的追加日志(心跳)的时间
	getVoteNum          int       //作为candidate所获得的选票数量

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 论文里figure2里都写了
type AppendEntriesArgs struct {
	Term         int     // 发起日志追加的server选期(其实未必是leader,但必然是曾经的leader)
	LeaderId     int     // 发起日志追加的server的id
	PrevLogIndex int     // 紧接在新条目之前的日志条目的索引(有点难理解),其实就是这次要追加给你的日志的第一条
	PreLogTerm   int     // PrevLogIndex的选期
	Entries      []Entry // 追加的日志
	LeaderCommit int     // 发起日志追加的server的提交位点
}

// 论文里figure2里都写了
type AppendEntriesReply struct {
	Term    int  // 当期回复心跳的server的选期(供leader感知外部)
	Success bool // 当期响应的的server是否已经包含了PrevLogIndex和PreLogTerm对应的条目(有点难理解)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 论文figure2里RequestVote RPC里的内容
	// 只有candidate才会发起选举

	// server个人信息
	Term        int // 候选人的选期
	CandidateId int //候选人的serverid
	// server的参选信息(接受者凭借参选信息决定是否投票)
	LastLogIndex int // 候选人的最新日志游标
	LastLogTerm  int // 候选人的最新日志选期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 论文figure2里RequestVote RPC里的内容

	Term        int  // 本server的当前选期,用来让candidate感知外部server的信息
	voteGranted bool //本server是否同意投票给源server
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对前来的投票请求进行分析
	// rule1: args 选期比当前的server的选期先进,当前server变身follower
	// rule2: args 的选期比当期server的选期要落后,拒绝投票给他,还得让他知道当期server的选期给他小刀拉屁股,让他开开眼
	// rule3: 当前server已经投票过了,但不是args的server,拒绝
	// rule4: 还没有投票,或者投过票了,查看是否符合投票准则,如果符合就投给当前主机,同时重置计时器 lastLogAppendTime,否则拒绝

	// rule1
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}

	// rule2
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.voteGranted = false
		return
	}

	// 后面都是args.Term==rf.currentTerm
	// rule3
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.voteGranted = false
		return
	}
	// rule4
	// 投票规则,对比LastLogIndex和LastLogTerm是否全面超越
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		if rf.log[lastLogIndex].Term > args.Term || (rf.log[lastLogIndex].Term == args.Term && lastLogIndex <= args.LastLogIndex) {
			reply.Term = rf.currentTerm
			reply.voteGranted = false
			return
		} else {
			reply.Term = rf.currentTerm
			reply.voteGranted = true
			rf.votedFor = args.CandidateId
			return
		}
	}

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.getVoteNum = 0

	rf.tickerElectionChan = make(chan bool)
	rf.tickerHeartbeatChan = make(chan bool)
	rf.lastApplied = 0
	rf.log = []Entry{}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.raftLoop()
	go rf.heartbeatTicker()
	go rf.electionTicker()

	return rf
}
