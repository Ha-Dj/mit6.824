package mr

const (
	TaskMap    = 1
	TaskReduce = 2
	TaskWait   = 3
	TaskDone   = 4
	TimeOut    = 20
)

type FileInfo struct {
	FileName string
	FileId   int
}

type MapTask struct {
	FileInfo FileInfo
	NReduce  int
}

type ReduceTask struct {
	FileInfo FileInfo
	HashId   int
}
