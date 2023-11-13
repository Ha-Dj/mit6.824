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
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// ---------------------------for debug---------------------------

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func PrettyDebug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

var statusMap = map[int]string{
	STATE_FOLLOWER:  "follower",
	STATE_LEADER:    "leader",
	STATE_CANDIDATE: "candidate",
}

// ---------------------------for debug---------------------------

const (
	Null                      = -1
	STATE_FOLLOWER            = 1
	STATE_CANDIDATE           = 2
	STATE_LEADER              = 3
	HeartBeatInterval         = 100
	AppendEntriesInterval     = 60
	ApplyInterval             = 10
	UpdateCommitIndexInterval = 5
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	voteFor     int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	status             int         // status
	electionTimer      *time.Timer // 选举超时重置
	appendEntriesTimer *time.Timer // 追加日志定时器
	heartBeatTimer     *time.Timer // 心跳定时器
	applyTimer         *time.Timer // apply定时器
	nPeers             int         // peers 结点个数
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == STATE_LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		fmt.Printf("Error: raft%d readPersist.", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.log = log
		var logLength = len(rf.log)
		fmt.Printf("%v raft%d readPersist, term:%d, voteFor:%d, logLength:%d\n", time.Now(), rf.me, rf.currentTerm, rf.voteFor, logLength)
		rf.nextIndex[rf.me] = logLength
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's index
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// you should hold lock when calling this func
func (rf *Raft) getLastLog() (int, LogEntry) {
	LastIndex := 0
	if rf.matchIndex[rf.me] >= 1 {
		LastIndex = rf.matchIndex[rf.me] - 1
	}
	return LastIndex, rf.log[LastIndex]
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) != 0 {
		PrettyDebug(dLog2, "S%d T%d received appendEntries rpc args:{Term%d, LeaderId%d, PrevLogIndex%d, "+
			"PrevLogTerm%d, len(Entries)%d, LeaderCommit%d}", rf.me, rf.currentTerm, args.Term, args.LeaderId,
			args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	}
	rf.electionTimer.Reset(randomElectionTimeout())
	// 如果 args's term < currentTerm, return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		PrettyDebug(dTerm, "S%d T%d converting to follower. Term too small1 (%d -> %d)", rf.me, rf.currentTerm,
			rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.voteFor = Null
		rf.status = STATE_FOLLOWER
		rf.persist()
	}

	// 获取最后一条日志的index 和 log 数据
	last_index := rf.nextIndex[rf.me] - 1

	// 如果此raft的最后一条日志的 index 小于 args 的 PrevLogIndex
	if last_index < args.PrevLogIndex {
		PrettyDebug(dLog2, "S%d LAST LOG NOT MATCH. (this.last_index %d < leader.Prev_index %d)", rf.me,
			last_index, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// args.PrevLogIndex 现在可能 等于 last_index 或者小于 last_index
	// an existing entry term 与 leader 最后一条日志的 term 不一致，直接删除
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		PrettyDebug(dLog2, "S%d LAST LOG NOT MATCH. (this.sameIndex_term %d != leader.Prev_term %d)", rf.me,
			rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		rf.log = rf.log[:args.PrevLogIndex]
		rf.persist()
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	PrettyDebug(dLog2, "S%d %s T%d append new log, len(rf.log) %d, args.PrevLogIndex %d", rf.me, statusMap[rf.status], rf.currentTerm, len(rf.log), args.PrevLogIndex)

	if args.PrevLogIndex+1 == len(rf.log) {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	} else {
		for i := 0; i < len(args.Entries); i++ {
			index := args.PrevLogIndex + 1 + i
			if index >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
			} else {
				rf.log[index] = args.Entries[i]
			}
		}
	}
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	PrettyDebug(dLog2, "S%d after append nextIndex%d matchIndex%d", rf.me, rf.nextIndex[rf.me], rf.matchIndex[rf.me])

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	PrettyDebug(dLog2, "S%d %s Term%d append new log, len %d", rf.me, statusMap[rf.status], rf.currentTerm, len(rf.log))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartBeat() {
	go func() {
		rf.mu.Lock()
		if rf.status != STATE_LEADER {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[rf.me] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[rf.me]-1].Term,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				rf.mu.Lock()
				if ok && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					PrettyDebug(dLeader, "S%d turn to follower : find bigger term", rf.me)
					rf.status = STATE_FOLLOWER
					rf.voteFor = Null
					rf.persist()
				}
				rf.mu.Unlock()
			}(index)
		}
	}()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = STATE_FOLLOWER
		rf.voteFor = Null
		rf.persist()
	}
	last_index := rf.nextIndex[rf.me] - 1
	last_log := rf.log[last_index]
	PrettyDebug(dVote, "S%d T%d received RequestVote from S%d T%d (args.LastLogTerm%d, last_log.Term%d, "+
		"args.LastLogIndex%d, last_index%d)", rf.me, rf.currentTerm, args.CandidateId, args.Term, args.LastLogTerm, last_log.Term, args.LastLogIndex, last_index)
	if (rf.voteFor == Null || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > last_log.Term || (args.LastLogTerm == last_log.Term && args.LastLogIndex >= last_index)) {
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		PrettyDebug(dVote, "S%d T%d Vote For S%d T%d", rf.me, rf.currentTerm, args.CandidateId,
			args.Term)
		rf.persist()
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.status != STATE_LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index = rf.nextIndex[rf.me]
	term = rf.currentTerm

	new_log := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, new_log)
	rf.persist()
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me]++

	PrettyDebug(dClient, "S%d %s Term %d append new log nextIndex%d matchIndex%d", rf.me, statusMap[rf.status],
		rf.currentTerm, rf.nextIndex[rf.me], rf.matchIndex[rf.me])

	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func randomElectionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(250)) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	PrettyDebug(dTimer, "S%d T%d start election", rf.me, rf.currentTerm)
	rf.currentTerm++
	rf.status = STATE_CANDIDATE
	rf.voteFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.nextIndex[rf.me] - 1,
		LastLogTerm:  rf.log[rf.nextIndex[rf.me]-1].Term,
	}
	rf.mu.Unlock()

	voteCh := make(chan bool, rf.nPeers-1)
	for index, _ := range rf.peers {
		if rf.me == index {
			continue
		}
		go func(index int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(index, &args, &reply)
			if !ok {
				voteCh <- false
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				PrettyDebug(dTerm, "S%d T%d converting to T%d follower", rf.me, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.status = STATE_FOLLOWER
				rf.voteFor = Null
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(index)
	}

	voteCnt := 1
	voteGrantedCnt := 1
	for voteGranted := range voteCh {
		rf.mu.Lock()
		if rf.status != STATE_CANDIDATE {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		voteCnt++

		if voteGranted {
			voteGrantedCnt++
		}

		if voteGrantedCnt > rf.nPeers/2 {
			rf.mu.Lock()
			rf.status = STATE_LEADER
			PrettyDebug(dLeader, "S%d T%d converting to Leader! ", rf.me, rf.currentTerm)
			rf.heartBeatTimer.Reset(0)
			go rf.updateCommitIndex()
			go rf.LeaderAppendEntries()
			rf.mu.Unlock()
			break
		}

		if voteCnt == len(rf.peers) {
			break
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.status == STATE_LEADER {
				rf.mu.Unlock()
				break
			}
			go rf.startElection()
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.status == STATE_LEADER {
				rf.startHeartBeat()
			}
			rf.heartBeatTimer.Reset(HeartBeatInterval * time.Millisecond)
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) applyTicker(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		select {
		case <-rf.applyTimer.C:
			rf.mu.Lock()
			rf.applyCheck(applyCh)
			rf.applyTimer.Reset(ApplyInterval * time.Millisecond)
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) LeaderAppendEntries() {
	for rf.killed() == false {
		time.Sleep(AppendEntriesInterval * time.Millisecond)
		rf.mu.Lock()
		if rf.status != STATE_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go func() {
			rf.mu.Lock()
			// 判断是否还是leader
			if rf.status != STATE_LEADER {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			wg := sync.WaitGroup{}
			// 向所有结点发送 log同步 rpc
			for index, _ := range rf.peers {
				rf.mu.Lock()
				// 判断是否还是leader
				if rf.status != STATE_LEADER {
					rf.mu.Unlock()
					break
				}
				// 如果是自身直接continue
				if index == rf.me {
					rf.mu.Unlock()
					continue
				}
				// 如果不需要发送直接continue
				if rf.nextIndex[rf.me]-1 < rf.nextIndex[index] {
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()

				// 对每个结点起一个 goroutine ：发送rpc 、处理rpc
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					//ok := false
					rf.mu.Lock()
					// 判断是否还是Leader
					if rf.status != STATE_LEADER {
						rf.mu.Unlock()
						return
					}
					// 生成发送的args
					LastIndex, LastLog := rf.getLastLog()
					args = AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: LastIndex,
						PrevLogTerm:  LastLog.Term,
						Entries:      make([]LogEntry, 0),
						LeaderCommit: rf.commitIndex,
					}
					args.Entries = append(args.Entries, rf.log[args.PrevLogIndex+1:]...)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(index, &args, &reply)
					if !ok {
						return
					}
					for {
						rf.mu.Lock()
						// rpc成功，判断是否还是最初的任期
						if rf.currentTerm != args.Term {
							rf.mu.Unlock()
							return
						}

						// 收到回复的Term大于当前rf的Term
						if reply.Term > rf.currentTerm {
							PrettyDebug(dLeader, "S%d T%d turn to follower: T%d -> T%d", rf.me, rf.currentTerm,
								rf.currentTerm, reply.Term)
							rf.status = STATE_FOLLOWER
							rf.voteFor = Null
							rf.currentTerm = reply.Term
							rf.persist()
							rf.mu.Unlock()
							return
						}

						if reply.Success {
							PrettyDebug(dWarn, "S%d %s T%d appendEntries success: log_len%d,", index,
								statusMap[index], rf.currentTerm, len(rf.log))
							rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
							rf.mu.Unlock()
							return
						} else {
							if args.PrevLogIndex >= 1 {
								PrettyDebug(dLeader, "S%d PrevLogIndex%d--", rf.me, args.PrevLogIndex)
								args.PrevLogIndex--
								args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
								args.Entries = rf.log[args.PrevLogIndex+1:]
							}
							rf.mu.Unlock()
							ok = rf.sendAppendEntries(index, &args, &reply)
							if !ok {
								return
							}
							rf.mu.Lock()
							// rpc成功，判断是否还是最初的任期
							if rf.currentTerm != args.Term {
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
						}
					}
				}(index)
			}
			wg.Wait()
		}()
	}
}

func (rf *Raft) updateCommitIndex() {
	for !rf.killed() {
		time.Sleep(UpdateCommitIndexInterval * time.Millisecond)
		rf.mu.Lock()
		if rf.status != STATE_LEADER {
			rf.mu.Unlock()
			return
		}
		for N := rf.nextIndex[rf.me] - 1; N > rf.commitIndex; N-- {
			if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
				count := 1
				for id, _ := range rf.peers {
					if id == rf.me {
						continue
					}
					if rf.matchIndex[id] >= N {
						PrettyDebug(dCommit, "S%d matchIndex > %d, count++", rf.commitIndex, N)
						count++
					}
				}
				if count >= rf.nPeers/2+1 {
					rf.commitIndex = N
					PrettyDebug(dCommit, "S%d commit %d", rf.me, rf.commitIndex)
					break
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyCheck(applyCh chan ApplyMsg) {
	go func() {
		rf.mu.Lock()
		var appliedMsgs = make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			PrettyDebug(dCommit, "S%d %s T%d commit{CommandIndex:%d}", rf.me, statusMap[rf.status],
				rf.currentTerm, rf.lastApplied)
			appliedMsgs = append(appliedMsgs, msg)
			applyCh <- msg
		}
		rf.mu.Unlock()
	}()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = Null
	rf.status = STATE_FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	for index, _ := range rf.nextIndex {
		rf.nextIndex[index] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	for index, _ := range rf.matchIndex {
		rf.matchIndex[index] = 0
	}
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nPeers = len(peers)
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(HeartBeatInterval * time.Millisecond)

	rf.appendEntriesTimer = time.NewTimer(AppendEntriesInterval * time.Millisecond)
	rf.applyTimer = time.NewTimer(ApplyInterval * time.Millisecond)
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	PrettyDebug(dLog, "S%d, making...", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	// start apply ticker goroutine to send applyMsg to applyCh
	go rf.applyTicker(applyCh)

	return rf
}
