package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	mapTask      *Queue
	mapCt        int
	reduceTask   *Queue
	reduceCt     int
	workIdToTask sync.Map
	ret          bool
}

var FinishedFlagTaskReply = TaskReply{
	Type: -1,
}

var NilFlagTaskReply = TaskReply{
	Type: -2,
}

func (c *Coordinator) initMapTask(files []string, nReduce int) {
	for i, file := range files {
		c.mapTask.Enqueue(&TaskReply{
			Type:           0,
			TaskID:         i,
			BucketNums:     nReduce,
			InputFileNames: []string{file},
		})
		c.mapCt++
	}
}

func (c *Coordinator) initReduceTask(files []string, nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.reduceTask.Enqueue(&TaskReply{
			Type:               1,
			TaskID:             i,
			TotalInputFileNums: len(files), // reade out-x-ID  0 <= x < InputFileSize
		})
		c.reduceCt++
	}
}

//	rpc call
//
// first fetch Map, if finished fetch Reduce
func (c *Coordinator) FetchTask(args *TaskArgs, reply *TaskReply) error {
	log.Printf("FetchTask(%v)", *args)

	//	*reply = *c.finishedFlagTask

	if !c.mapTask.Empty() {
		task, ok := c.mapTask.Dequeue().(*TaskReply)
		if !ok {
			fmt.Printf("err type of map task dequeue")
			return nil
		}
		// todo need a atomic
		*reply = *task
		c.workIdToTask.Store(args.WorkerID, reply)
		c.workIdToTask.Range(func(workID, taskReply interface{}) bool {
			log.Printf("Worker ID: %d, Task Reply: %+v\n", workID, taskReply)
			return true

		})
		return nil
	}

	if c.mapCt > 0 {
		// map not complete finished but queue is empty
		*reply = NilFlagTaskReply
		return nil
	}

	if !c.reduceTask.Empty() {
		task, ok := c.reduceTask.Dequeue().(*TaskReply)
		if !ok {
			fmt.Printf("err type of reduce task dequeue")
			return nil
		}
		*reply = *task
		c.workIdToTask.Store(args.WorkerID, reply)
		c.workIdToTask.Range(func(workID, taskReply interface{}) bool {
			log.Printf("Worker ID: %d, Task Reply: %+v\n", workID, taskReply)
			return true

		})
		return nil
	}
	log.Printf("FetchTask(%v), all task is finished", *args)
	*reply = FinishedFlagTaskReply
	return nil
}

// call back finished
func (c *Coordinator) CallBackTask(args *CallBackArgs, reply *CallBackReply) error {

	workID := args.WorkerID

	// todo check status
	taskInterface, ok := c.workIdToTask.Load(workID)
	if !ok {
		log.Printf("Worker ID: %d not found\n", workID)
		*reply = CallBackReply{
			Status: -1,
		}
	} else {
		c.workIdToTask.Delete(workID)
		task, _ := taskInterface.(*TaskReply)
		if task.Type == 0 {
			c.mapCt--
		} else {
			c.reduceCt--
		}
		log.Printf("Remove Worker ID: %d, task: %+v, mapCt: %d, reduceCt: %d\n", workID, task, c.mapCt, c.reduceCt)
		// call to set finished status
		c.isFinished()
	}

	*reply = CallBackReply{
		Status: 1,
	}
	return nil
}

func (c *Coordinator) isFinished() {
	c.ret = c.mapTask.Empty() && c.reduceTask.Empty() && !c.hasRunningTask()
}

func (c *Coordinator) hasRunningTask() bool {
	empty := true
	c.workIdToTask.Range(func(k, v interface{}) bool {
		empty = false
		return false
	})
	return !empty
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
	return c.ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTask:      NewQueue(),
		reduceTask:   NewQueue(),
		workIdToTask: sync.Map{},
	}

	c.initMapTask(files, nReduce)
	c.initReduceTask(files, nReduce)
	// Your code here.
	// 拆分任务

	c.server()
	return &c
}
