package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader  int
	peerNum int
	clerkID int64
	seqID   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.seqID = 0
	ck.leader = 0
	ck.peerNum = len(ck.servers)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		ClerkID: ck.clerkID,
		SeqID:   atomic.AddInt64(&ck.seqID, 1),
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok {
			// 轮询
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else {
			if reply.Err == "NOT LEADER" {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			if reply.Err == "OK" {
				return reply.Value
			}
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkID: ck.clerkID,
		SeqID:   atomic.AddInt64(&ck.seqID, 1),
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			// 轮询
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else {
			if reply.Err == "NOT LEADER" {
				DPrintf("WARN %d NOT LEADER", ck.leader)
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			if reply.Err == "OK" {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Client Put (%s, %s)", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("Client Append (%s, %s)", key, value)
	ck.PutAppend(key, value, "Append")
}
