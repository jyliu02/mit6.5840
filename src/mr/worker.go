package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	workerId := os.Getpid()

	for {
		task := requestTask(workerId)

		switch task.TaskType {
		case "map":
			performMapTask(task, mapf)
			reportTaskCompleted(workerId, "map", task.TaskId)
		case "reduce":
			performReduceTask(task, reducef)
			reportTaskCompleted(workerId, "reduce", task.TaskId)
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		default:
			log.Fatalf("Unknown task type: %v", task.TaskType)
		}
	}
}

func requestTask(workerId int) RequestTaskReply {
	args := RequestTaskArgs{WorkerId: workerId}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Fatal("Failed to request task from coordinator")
	}

	return reply
}

func reportTaskCompleted(workerId int, taskType string, taskId int) {
	args := TaskCompletedArgs{
		TaskType: taskType,
		TaskId:   taskId,
		WorkerId: workerId,
	}
	reply := TaskCompletedReply{}

	ok := call("Coordinator.TaskCompleted", &args, &reply)
	if !ok {
		log.Printf("Failed to report task completion")
	}
}

func performMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	// Read input file
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("Cannot read file %v: %v", task.FileName, err)
	}

	// Apply map function
	kva := mapf(task.FileName, string(content))

	// Create intermediate files for each reduce task
	intermediateFiles := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	// Create temp files to avoid partial writes
	tempNames := make([]string, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mr-map-")
		if err != nil {
			log.Fatalf("Cannot create temp file: %v", err)
		}
		intermediateFiles[i] = tempFile
		tempNames[i] = tempFile.Name()
		encoders[i] = json.NewEncoder(tempFile)
	}

	// Partition key-value pairs by reduce task number
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		err := encoders[reduceTaskNum].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode key-value pair: %v", err)
		}
	}

	// Close temp files and rename them atomically
	for i := 0; i < task.NReduce; i++ {
		intermediateFiles[i].Close()

		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		err := os.Rename(tempNames[i], finalName)
		if err != nil {
			log.Fatalf("Cannot rename temp file: %v", err)
		}
	}
}

func performReduceTask(task RequestTaskReply, reducef func(string, []string) string) {
	// Read all intermediate files for this reduce task
	var intermediate []KeyValue

	for mapTaskId := 0; mapTaskId < task.NMap; mapTaskId++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapTaskId, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Cannot open intermediate file %v: %v", fileName, err)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort by key
	sort.Sort(ByKey(intermediate))

	// Create temp output file
	tempFile, err := os.CreateTemp(".", "mr-out-")
	if err != nil {
		log.Fatalf("Cannot create temp output file: %v", err)
	}
	tempName := tempFile.Name()

	// Process keys and write output
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

		// Write in the correct format for reduce output
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	// Atomically rename temp file to final output
	finalName := fmt.Sprintf("mr-out-%d", task.TaskId)
	err = os.Rename(tempName, finalName)
	if err != nil {
		log.Fatalf("Cannot rename output file: %v", err)
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
func call(rpcname string, args any, reply any) bool {
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
