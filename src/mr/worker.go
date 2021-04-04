package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapWorker(filename string, nReduce int, mapf func(string, string) []KeyValue, index int, taskId int) error {
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("%v cannot open %v", 50, filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("%v cannot read %v", 54, filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	intermediate_file := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		// create temp files for intermediate result
		ofile, _ := ioutil.TempFile(".", "temp-"+strconv.Itoa(index)+"-"+strconv.Itoa(i))
		intermediate_file[i] = ofile
	}

	encoders := make([]*json.Encoder, nReduce)
	// create encoders
	for i := 0; i < nReduce; i++ {
		encoders[i] = json.NewEncoder(intermediate_file[i])
	}
	for i := 0; i < len(intermediate); i++ {
		// this is the correct format for each line of Reduce output.
		kv := intermediate[i]
		hash := ihash(intermediate[i].Key) % nReduce
		encoders[hash].Encode(&kv)
	}

	reply := MapTaskDoneReply{}
	args := MapTaskDoneArgs{}
	args.Index = index
	args.MapTaskId = taskId
	call("Master.MapTaskDone", &args, &reply)

	if reply.Success == false {
		// this worker misses the deadline and the task
		// has been assigned to another worker.
		for i := 0; i < nReduce; i++ {
			os.Remove(intermediate_file[i].Name())
		}
		return nil
	}

	for i := 0; i < nReduce; i++ {
		// atomically rename the temp file so reduce worker will not read the intermediate value
		os.Rename(intermediate_file[i].Name(), "./mr-"+strconv.Itoa(index)+"-"+strconv.Itoa(i))
		intermediate_file[i].Close()
	}

	return nil
}

func ReduceWorker(nReduce int, reducef func(string, []string) string, reduceTaskId int, totalMapNum int) error {
	intermediate := []KeyValue{}

	// read mr-*-nReduce files and save result in intermediate
	for i := 0; i < totalMapNum; i++ {
		var file *os.File
		var err error
		for {
			// try openning again and again because map workers may be saving the result
			file, err = os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(nReduce))
			if err == nil {
				break
			}
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// create a temp file
	ofile, _ := ioutil.TempFile(".", "temp-reduce")

	// do the reduce job
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	reply := ReduceTaskDoneReply{}
	args := ReduceTaskDoneArgs{}
	args.NReduce = nReduce
	args.ReduceTaskId = reduceTaskId
	call("Master.ReduceTaskDone", &args, &reply)

	if reply.Success == false {
		// remember to remove temp file if not success
		defer os.Remove(ofile.Name())
		return nil
	}

	oname := "mr-out-" + strconv.Itoa(nReduce)

	os.Rename(ofile.Name(), oname)
	ofile.Close()
	return nil
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		reply := GetTaskReply{}
		args := ExampleArgs{}
		err := call("Master.GetTask", &args, &reply)
		if !err {
			// master exits or crashes
			return
		}

		if reply.AllTaskDone {
			break
		}

		if reply.HasTask {
			if reply.Tasktype == MapTask {
				MapWorker(reply.Filename, reply.NReduce, mapf, reply.Index, reply.TaskId)
			} else if reply.Tasktype == ReduceTask {
				ReduceWorker(reply.NReduce, reducef, reply.TaskId, reply.TotalMapNum)
			} else {
				fmt.Println("Invalid task type!")
				return
			}
		}
		time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
