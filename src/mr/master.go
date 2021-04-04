package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	files          []string // input files
	allocatedFiles []bool   // true if the file has been assigned to a worker
	doneFiles      []bool   // true if the map task has been done
	mapTaskId      []int    // current worker id responsible for the map task

	doneMapNum  int // the num of map tasks that have been done
	totalMapNum int // should equal to len(files)

	allocatedReduceNums []bool // true if the reduce task has been assigned to a worker
	doneReduceTasks     []bool // true if the reduce task has been done
	reduceTaskId        []int  // current worker id responsible for the reduce task

	doneReduceNum  int // the num of map tasks that have been done
	totalReduceNum int // should equal to the nReduce argument of makeMaster

	mutex sync.Mutex // mutex for syncronization

	nReduce int // the total num of reduce tasks
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *ExampleArgs, reply *GetTaskReply) error {
	reply.AllTaskDone = false
	if m.doneMapNum == m.totalMapNum {
		if m.doneReduceNum == m.nReduce {
			// all map tasks and reduce tasks have been done
			reply.AllTaskDone = true
			return nil
		}
		return m.GetReduceTask(args, reply)
	}
	return m.GetMapTask(args, reply)
}

func (m *Master) GetMapTask(args *ExampleArgs, reply *GetTaskReply) error {
	m.mutex.Lock()
	// look up a map task that has not been assigned yet
	var index int = -1
	for i := 0; i < len(m.allocatedFiles); i++ {
		if m.allocatedFiles[i] == false {
			m.allocatedFiles[i] = true
			index = i
			break
		}
	}
	// all map tasks have been assigned
	if index == -1 {
		reply.HasTask = false
		m.mutex.Unlock()
		return nil
	}

	reply.HasTask = true
	reply.Index = index
	reply.Tasktype = MapTask
	reply.Filename = m.files[index]
	reply.NReduce = m.nReduce
	m.mapTaskId[index] += 1
	reply.TaskId = m.mapTaskId[index] // allocate a task id for the worker
	m.mutex.Unlock()

	go m.WaitMapTaskDone(index)
	return nil
}

func (m *Master) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	m.mutex.Lock()
	if args.MapTaskId != m.mapTaskId[args.Index] {
		// this task has been assigned to another work, just ignore it
		reply.Success = false
		m.mutex.Unlock()
		return nil
	}

	// the reason we set m.allocatedFiles[args.Index] as true is that:
	// If a worker misses the deadline (10 seconds for our case) but it still
	// finished its task at last, wo don't want to allocate this task to another task
	m.allocatedFiles[args.Index] = true
	m.doneFiles[args.Index] = true
	m.doneMapNum += 1
	reply.Success = true
	m.mutex.Unlock()

	return nil
}

func (m *Master) WaitMapTaskDone(index int) {
	// wait for 10 seconds first
	time.Sleep(10 * time.Second)

	m.mutex.Lock()
	if m.doneFiles[index] == false {
		// if the map task has still not been done, the worker
		// may be crashed, mark m.allocatedFiles[] as false so
		// a new worker can get this task
		m.allocatedFiles[index] = false
	}
	m.mutex.Unlock()
}

func (m *Master) GetReduceTask(args *ExampleArgs, reply *GetTaskReply) error {
	m.mutex.Lock()

	var index int = -1
	for i := 0; i < m.nReduce; i++ {
		if m.allocatedReduceNums[i] == false {
			m.allocatedReduceNums[i] = true
			index = i
			break
		}
	}

	if index == -1 {
		reply.HasTask = false
		m.mutex.Unlock()
		return nil
	}

	reply.HasTask = true
	reply.NReduce = index
	m.reduceTaskId[index] += 1
	reply.Tasktype = ReduceTask
	reply.TaskId = m.reduceTaskId[index]
	reply.TotalMapNum = m.totalMapNum
	m.mutex.Unlock()

	go m.WaitReduceTaskDone(index)
	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	m.mutex.Lock()
	if args.ReduceTaskId != m.reduceTaskId[args.NReduce] {
		// this task has been assigned to another work, just ignore it
		reply.Success = false
		m.mutex.Unlock()
		return nil
	}

	m.allocatedReduceNums[args.NReduce] = true
	m.doneReduceTasks[args.NReduce] = true
	m.doneReduceNum += 1
	reply.Success = true
	m.mutex.Unlock()

	return nil
}

func (m *Master) WaitReduceTaskDone(index int) {
	time.Sleep(10 * time.Second)

	m.mutex.Lock()
	if m.doneReduceTasks[index] == false {
		// if the reduce task has still not been done, the worker
		// may be crashed, mark m.allocatedReduceNums[] as false so
		// a new worker can get this task
		m.allocatedReduceNums[index] = false
	}
	m.mutex.Unlock()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// the master process should exit when all reduce tasks have been done
	ret := m.doneReduceNum == m.nReduce

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	len := len(files)
	m.files = files
	m.nReduce = nReduce
	m.totalMapNum = len
	m.doneFiles = make([]bool, len)
	m.allocatedFiles = make([]bool, len)
	m.mapTaskId = make([]int, len)

	m.allocatedReduceNums = make([]bool, nReduce)
	m.doneReduceTasks = make([]bool, nReduce)
	m.reduceTaskId = make([]int, nReduce)

	m.server()
	return &m
}
