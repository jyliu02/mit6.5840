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

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	// input files
	inputFiles []string
	// number of reduce tasks
	nReduce int

	// map task status
	mapTasks []int // 0: not started, 1: in progress, 2: completed
	// reduce task status
	reduceTasks []int // 0: not started, 1: in progress, 2: completed

	// indicates whether all tasks are done
	done bool
}

// Your code here -- RPC handlers for the worker to call.

const TaskTimeout = 10 * time.Second

// startTaskTimeout launches a goroutine that will reset the task status if it doesn't complete in time
func (c *Coordinator) startTaskTimeout(taskType string, taskId int) {
	go func() {
		time.Sleep(TaskTimeout)

		c.mu.Lock()
		defer c.mu.Unlock()

		switch taskType {
		case "map":
			// Only reset if task is still in progress (status == 1)
			if taskId < len(c.mapTasks) && c.mapTasks[taskId] == 1 {
				log.Printf("Map task %d timed out, resetting to available", taskId)
				c.mapTasks[taskId] = 0 // reset to not started
			}
		case "reduce":
			// Only reset if task is still in progress (status == 1)
			if taskId < len(c.reduceTasks) && c.reduceTasks[taskId] == 1 {
				log.Printf("Reduce task %d timed out, resetting to available", taskId)
				c.reduceTasks[taskId] = 0 // reset to not started
			}
		}
	}()
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// assign map tasks first
	for i, status := range c.mapTasks {
		if status == 0 {
			// assign map task
			c.mapTasks[i] = 1 // mark as in progress
			reply.TaskType = "map"
			reply.TaskId = i
			reply.FileName = c.inputFiles[i]
			reply.NReduce = c.nReduce

			// Start timeout goroutine for this task
			c.startTaskTimeout("map", i)

			log.Printf("Assigned map task %d to worker %d", i, args.WorkerId)
			return nil
		}
	}

	// check if all map tasks are completed
	allMapDone := true
	for _, status := range c.mapTasks {
		if status != 2 {
			allMapDone = false
			break
		}
	}
	if !allMapDone {
		reply.TaskType = "wait"
		return nil
	}

	// only assign reduce tasks if all map tasks are done
	for i, status := range c.reduceTasks {
		if status == 0 {
			// assign reduce task
			c.reduceTasks[i] = 1 // mark as in progress
			reply.TaskType = "reduce"
			reply.TaskId = i
			reply.NMap = len(c.inputFiles)

			// Start timeout goroutine for this task
			c.startTaskTimeout("reduce", i)

			log.Printf("Assigned reduce task %d to worker %d", i, args.WorkerId)
			return nil
		}
	}

	// check if all reduce tasks are completed
	allReduceDone := true
	for _, status := range c.reduceTasks {
		if status != 2 {
			allReduceDone = false
			break
		}
	}
	if allReduceDone {
		reply.TaskType = "exit"
		c.done = true
		log.Printf("All tasks completed, signaling exit")
		return nil
	}

	// if all reduce tasks are in progress, ask worker to wait, in case some reduce tasks fail
	reply.TaskType = "wait"
	return nil
}

func (c *Coordinator) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case "map":
		if args.TaskId < len(c.mapTasks) {
			c.mapTasks[args.TaskId] = 2 // mark map task as completed
			log.Printf("Map task %d completed by worker %d", args.TaskId, args.WorkerId)
		}
	case "reduce":
		if args.TaskId < len(c.reduceTasks) {
			c.reduceTasks[args.TaskId] = 2 // mark reduce task as completed
			log.Printf("Reduce task %d completed by worker %d", args.TaskId, args.WorkerId)
		}
	}

	reply.Ack = true
	return nil
}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce

	c.mapTasks = make([]int, len(files))
	c.reduceTasks = make([]int, nReduce)

	c.done = false

	// start the RPC server in a new goroutine.
	c.server()
	return &c
}
