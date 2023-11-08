package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	workerId   int
	workerType int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		request := AskTaskRequest{}
		reply := AskTaskReply{}
		call("Coordinator.AskTask", &request, &reply)
		// 1. MapTask
		switch reply.TaskType {
		case TaskMap:
			doMapTask(mapf, reply)
			break
		case TaskReduce:
			doReduceTask(reducef, reply)
			break
		case TaskWait:
			time.Sleep(1 * time.Second)
			break
		case TaskDone:
			return
		default:
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doMapTask(mapf func(string, string) []KeyValue, reply AskTaskReply) {
	fileInfo := reply.MapTask.FileInfo
	file, err := os.Open(fileInfo.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileInfo.FileName)
	}
	file.Close()
	kvs := mapf(fileInfo.FileName, string(content))
	sort.Sort(ByKey(kvs))
	kvReduce := make(map[int][]KeyValue)
	for _, item := range kvs {
		i := ihash(item.Key) % reply.MapTask.NReduce
		kvReduce[i] = append(kvReduce[i], item)
	}
	doneArgs := AskDoneArgs{TaskType: TaskMap, MapTask: reply.MapTask, ReduceTask: make([]ReduceTask, 0), RedHashId: -1}
	for Y := range kvReduce {
		output := "mr-" + strconv.Itoa(fileInfo.FileId) + "-" + strconv.Itoa(Y)
		file, err = os.Create(output)
		if err != nil {
			log.Fatalf("cannot create %v", output)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvReduce[Y] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot Encode kv %v", output)
			}
		}
		file.Close()
		RedFileInfo := FileInfo{output, fileInfo.FileId}
		redTask := ReduceTask{RedFileInfo, Y}
		doneArgs.ReduceTask = append(doneArgs.ReduceTask, redTask)
	}
	DoneReply := AskDoneReply{}
	call("Coordinator.TaskDoneReply", &doneArgs, &DoneReply)
}

func doReduceTask(reducef func(string, []string) string, reply AskTaskReply) {
	if len(reply.RedTask) == 0 {
		log.Fatalf("reply.RedTask is Empty!")
		return
	}
	hashIdx := reply.RedTask[0].HashId
	oname := "mr-out-" + strconv.Itoa(hashIdx)

	kvs := []KeyValue{}

	for _, task := range reply.RedTask {
		file, err := os.Open(task.FileInfo.FileName)
		if err != nil {
			log.Fatalf("Can not open %v", task.FileInfo.FileName)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	sort.Sort(ByKey(kvs))
	tmp_file, tmp_err := os.CreateTemp(".", oname)
	if tmp_err != nil {
		log.Fatalf("Can not create TempFile!")
		return
	}
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp_file, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	tmp_file.Close()
	os.Rename(tmp_file.Name(), oname)
	doneArgs := AskDoneArgs{TaskType: TaskReduce, MapTask: reply.MapTask, ReduceTask: make([]ReduceTask, 0), RedHashId: hashIdx}
	DoneReply := AskDoneReply{}
	call("Coordinator.TaskDoneReply", &doneArgs, &DoneReply)
}
