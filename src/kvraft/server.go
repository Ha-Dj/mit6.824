package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const (
	ErrNotLeader = "NOT LEADER"
	ErrOK        = "OK"
	GetOp        = "Get"
	PutOp        = "Put"
	AppendOp     = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	Key     string
	Value   string
	ClerkID int64
	SeqID   int64
	Index   int // raft服务层传回来的Index
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	SeqMap    map[int64]int64
	waitChMap map[int]chan Op
	KVData    map[string]string
}

func (kv *KVServer) GetWaitCh(LogIndex int) chan Op {
	ch, exist := kv.waitChMap[LogIndex]
	if !exist {
		kv.waitChMap[LogIndex] = make(chan Op, 1)
		ch = kv.waitChMap[LogIndex]
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrNotLeader
		return
	}

	op := Op{
		OpType:  GetOp,
		Key:     args.Key,
		ClerkID: args.ClerkID,
		SeqID:   args.SeqID,
	}

	LogIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}
	kv.mu.Lock()
	ch := kv.GetWaitCh(LogIndex)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()
	timer := time.NewTimer(150 * time.Millisecond)

	select {
	case <-timer.C:
		reply.Err = ErrNotLeader
		return
	case replyOp := <-ch:
		if replyOp.ClerkID != op.ClerkID || replyOp.SeqID != op.SeqID {
			reply.Err = ErrNotLeader
			return
		}
		reply.Err = ErrOK
		kv.mu.Lock()
		reply.Value = kv.KVData[args.Key]
		kv.mu.Unlock()
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrNotLeader
		return
	}

	op := Op{
		OpType:  args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkID,
		SeqID:   args.SeqID,
	}

	LogIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	kv.mu.Lock()
	ch := kv.GetWaitCh(LogIndex)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTimer(100 * time.Millisecond)

	select {
	case replyOp := <-ch:
		if op.ClerkID != replyOp.ClerkID || op.SeqID != replyOp.SeqID {
			reply.Err = ErrNotLeader
			return
		}
		reply.Err = ErrOK
		return
	case <-timer.C:
		reply.Err = ErrNotLeader
		return
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ifDuplicate(clerkID int64, seqID int64) bool {
	lastSeqIndex, exist := kv.SeqMap[clerkID]
	if !exist {
		return false
	}
	return lastSeqIndex >= seqID
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				LogIndex := msg.CommandIndex
				if !kv.ifDuplicate(op.ClerkID, op.SeqID) {
					if op.OpType == PutOp {
						kv.KVData[op.Key] = op.Value
					}
					if op.OpType == AppendOp {
						DPrintf("Before (%s, %s) Append (%s, %s)", op.Key, op.Value, op.Key, kv.KVData[op.Key])
						kv.KVData[op.Key] += op.Value
						DPrintf("After (%s, %s) Append (%s, %s)", op.Key, op.Value, op.Key, kv.KVData[op.Key])
					}
					kv.SeqMap[op.ClerkID] = op.SeqID
				}
				op.Index = msg.CommandIndex
				kv.GetWaitCh(LogIndex) <- op
				kv.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.SeqMap = make(map[int64]int64)
	kv.KVData = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	go kv.applyMsgHandlerLoop()

	return kv
}
