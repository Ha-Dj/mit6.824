package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mutex       sync.Mutex
	isDone      bool
	nReduce     int
	mapTaskQ    []MapTask
	mapTaskingQ []MapTask
	RedTaskM    map[int][]FileInfo
	RedTaskingM map[int][]FileInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{RedTaskM: make(map[int][]FileInfo), RedTaskingM: make(map[int][]FileInfo), nReduce: nReduce}
	c.isDone = false
	c.mapTaskQ = make([]MapTask, 0)
	c.mapTaskingQ = make([]MapTask, 0)
	// Your code here.
	// 1. 全部入mapTaskQ
	for i, file := range files {
		meta := FileInfo{file, i}
		task := MapTask{FileInfo: meta, NReduce: nReduce}
		c.mapTaskQ = append(c.mapTaskQ, task)
	}

	go c.ReAssignTask()

	c.server()
	return &c
}

func (c *Coordinator) AskTask(request *AskTaskRequest, reply *AskTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// mapTaskQ 非空，且够分配
	if len(c.mapTaskQ) > 0 {
		reply.TaskType = TaskMap
		task := c.mapTaskQ[0]
		c.mapTaskQ = append(c.mapTaskQ[:0], c.mapTaskQ[1:]...)
		c.mapTaskingQ = append(c.mapTaskingQ, task)
		reply.MapTask = task
		return nil
	}
	if len(c.mapTaskingQ) > 0 {
		reply.TaskType = TaskWait
		return nil
	}
	if len(c.RedTaskM) > 0 {
		reply.TaskType = TaskReduce
		hashId := -1
		for k, v := range c.RedTaskM {
			if len(v) != 0 {
				hashId = k
				break
			}
		}
		// 将此 hashId 移动至 tasking 队列
		c.RedTaskingM[hashId] = c.RedTaskM[hashId]
		delete(c.RedTaskM, hashId)
		// 加入到 reply 的 RedTask
		for _, fileinfo := range c.RedTaskingM[hashId] {
			reply.RedTask = append(reply.RedTask, ReduceTask{fileinfo, hashId})
		}
		// fmt.Println(reply.RedTask)
		return nil
	}
	if len(c.RedTaskingM) > 0 {
		// fmt.Println("Tasking Reduce...")
		reply.TaskType = TaskWait
		return nil
	}
	// fmt.Println("TaskEnd!")
	reply.TaskType = TaskDone
	c.isDone = true
	return nil
}

//func (c *Coordinator) AssignMapTask() MapTask {
//	task := c.mapTaskQ[0]
//	c.mapTaskQ = append(c.mapTaskQ[:0], c.mapTaskQ[1:]...)
//	c.mapTaskingQ = append(c.mapTaskingQ, task)
//	return task
//}

func (c *Coordinator) ReAssignTask() {
	for {
		// 休眠一段时间
		time.Sleep(TimeOut * time.Second)
		c.mutex.Lock()
		// 休眠结束后如果还有未处理完的任务， 全部加入待处理队列重新处理
		if len(c.mapTaskingQ) > 0 {
			for _, task := range c.mapTaskingQ {
				c.mapTaskQ = append(c.mapTaskQ, task)
			}
			c.mapTaskingQ = make([]MapTask, 0)
		}
		if len(c.RedTaskingM) > 0 {
			for key, task := range c.RedTaskingM {
				c.RedTaskM[key] = task
			}
			c.RedTaskingM = make(map[int][]FileInfo)
		}
		c.mutex.Unlock()
	}
}

func (c *Coordinator) TaskDoneReply(DoneArgs *AskDoneArgs, reply *AskDoneReply) error {
	if DoneArgs.TaskType == TaskMap {
		c.mutex.Lock()
		// 将此 mapTask 从 mapTaskingQ 中删除
		for i := 0; i < len(c.mapTaskingQ); i++ {
			if c.mapTaskingQ[i].FileInfo.FileId == DoneArgs.MapTask.FileInfo.FileId {
				c.mapTaskingQ = append(c.mapTaskingQ[:i], c.mapTaskingQ[i+1:]...)
			}
		}
		// 更新的redTaskM
		for _, redTask := range DoneArgs.ReduceTask {
			c.RedTaskM[redTask.HashId] = append(c.RedTaskM[redTask.HashId], redTask.FileInfo)
		}
		c.mutex.Unlock()
		return nil
	}
	if DoneArgs.TaskType == TaskReduce {
		c.mutex.Lock()
		// 将当前hashid 对应的 key 从 RedTaskingM中删除
		delete(c.RedTaskingM, DoneArgs.RedHashId)
		c.mutex.Unlock()
		return nil
	} else {
		log.Fatalf("UnknownType send DoneArgs!")
		return nil
	}
}
