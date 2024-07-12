package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTask          *Queue
	reduceTask       *Queue
	workIdToTask     map[int]*TaskReply
	finishedFlagTask *TaskReply
}

func (c *Coordinator) initMapTask(files []string, nReduce int) {
	for i, file := range files {
		c.mapTask.Enqueue(TaskReply{
			Type:          0,
			ID:            i,
			FileNums:      nReduce,
			InputFileName: file,
		})
	}
}

func (c *Coordinator) initReduceTask(files []string, nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.reduceTask.Enqueue(TaskReply{
			Type:          1,
			ID:            i,
			InputFileSize: len(files), // reade out-x-ID  0 <= x < InputFileSize
		})
	}
}

//	rpc call
//
// first fetch Map, if finished fetch Reduce
func (c *Coordinator) FetchTask(workId *TaskArgs, reply *TaskReply) {

	reply = c.finishedFlagTask

	if !c.mapTask.Empty() {
		reply, ok := c.mapTask.Dequeue().(*TaskReply)
		if !ok {
			fmt.Printf("err type of map task dequeue")
			return
		}
		c.workIdToTask[workId.ID] = reply
		return
	}

	if !c.reduceTask.Empty() {
		reply, ok := c.reduceTask.Dequeue().(*TaskReply)
		if !ok {
			fmt.Printf("err type of reduce task dequeue")
			return
		}
		c.workIdToTask[reply.ID] = reply
		return
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTask:      NewQueue(),
		reduceTask:   NewQueue(),
		workIdToTask: make(map[int]*TaskReply),
		finishedFlagTask: &TaskReply{
			Type: -1,
		},
	}

	c.initMapTask(files, nReduce)
	c.initReduceTask(files, nReduce)
	// Your code here.
	// 拆分任务

	c.server()
	return &c
}
