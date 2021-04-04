package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetMapTaskReply struct {
	Filename    string
	Index       int
	NReduce     int
	HasTask     bool
	AllTaskDone bool
	MapTaskId   int
}

type MapTaskDoneArgs struct {
	Index     int
	MapTaskId int
}

type MapTaskDoneReply struct {
	Success bool
}

type GetRedueTaskReply struct {
	NReduce      int
	HasTask      bool
	AllTaskDone  bool
	ReduceTaskId int
	TotalMapNum  int
}

const (
	MapTask = iota + 1
	ReduceTask
)

type TaskType int

type GetTaskReply struct {
	Tasktype TaskType

	// map task specific
	Filename string
	Index    int

	// reduce task specific
	TotalMapNum int

	// in map task, it represents the toal NReduce num
	// in reduce task, it represents task's reduce num
	NReduce     int
	HasTask     bool
	TaskId      int
	AllTaskDone bool
}

type ReduceTaskDoneArgs struct {
	NReduce      int
	ReduceTaskId int
}

type ReduceTaskDoneReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
