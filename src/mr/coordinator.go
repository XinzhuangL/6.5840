package mr

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	mapTask      *Queue
	mapCt        int32
	reduceTask   *Queue
	reduceCt     int32
	workIdToTask sync.Map // int -> TaskReply
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
		atomic.AddInt32(&c.mapCt, 1)
	}
	log.Printf("init map task finished: %v", c.mapCt)
}

func (c *Coordinator) initReduceTask(files []string, nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.reduceTask.Enqueue(&TaskReply{
			Type:               1,
			TaskID:             i,
			TotalInputFileNums: len(files), // reade out-x-ID  0 <= x < InputFileSize
		})
		atomic.AddInt32(&c.reduceCt, 1)
	}
	log.Printf("init reduce task finished: %v", c.reduceCt)
}

//	rpc call
//
// first fetch Map, if finished fetch Reduce
func (c *Coordinator) FetchTask(args *TaskArgs, reply *TaskReply) error {
	log.Printf("Receive FetchTask(%v)", *args)

	//	*reply = *c.finishedFlagTask

	if !c.mapTask.Empty() {
		task, ok := c.mapTask.Dequeue().(*TaskReply)
		task.DispatchTime = time.Now()
		if !ok {
			fmt.Printf("err type of map task dequeue")
			return nil
		}
		// todo need a atomic
		// todo too time not return
		*reply = *task
		c.workIdToTask.Store(args.WorkerID, reply)
		//c.workIdToTask.Range(func(workID, taskReply interface{}) bool {
		//	log.Printf("Worker ID: %d, Task Reply: %+v\n", workID, taskReply)
		//	return true
		//
		//})
		log.Printf("Receive FetchTask(%v), fetch a map task: %v", args, reply)
		return nil
	}

	if c.mapCt > 0 {
		// map not complete finished but queue is empty
		*reply = NilFlagTaskReply
		log.Printf("Receive FetchTask(%v), All map is dispatch but not finish, mapCt %d", args, c.mapCt)
		return nil
	}

	if !c.reduceTask.Empty() {
		task, ok := c.reduceTask.Dequeue().(*TaskReply)
		task.DispatchTime = time.Now()
		if !ok {
			fmt.Printf("err type of reduce task dequeue")
			return nil
		}
		*reply = *task
		c.workIdToTask.Store(args.WorkerID, reply)
		//c.workIdToTask.Range(func(workID, taskReply interface{}) bool {
		//	log.Printf("Worker ID: %d, Task Reply: %+v\n", workID, taskReply)
		//	return true
		//
		//})
		log.Printf("Receive FetchTask(%v), fetch a reduce task: %v", args, reply)
		return nil
	}
	log.Printf("FetchTask(%v), all task is finished", *args)
	*reply = FinishedFlagTaskReply
	return nil
}

// call back finished
func (c *Coordinator) CallBackTask(args *CallBackArgs, reply *CallBackReply) error {
	log.Printf("Receive CallBackTask(%v), It's finished", *args)

	workID := args.WorkerID

	// todo check status
	taskInterface, ok := c.workIdToTask.Load(workID)
	if !ok {
		log.Printf("Worker ID: %d not found, CallBackArgs: %v \n", workID, args)
		*reply = CallBackReply{
			Status: -1,
		}
	} else {
		c.workIdToTask.Delete(workID)
		task, _ := taskInterface.(*TaskReply)
		if task.Type == 0 {
			atomic.AddInt32(&c.mapCt, -1)
		} else {
			atomic.AddInt32(&c.reduceCt, -1)
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
	go func() {

		log.Printf("start coordinator timeout checker")
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.workIdToTask.Range(func(workID, taskReply interface{}) bool {
					task := taskReply.(*TaskReply)
					log.Printf("TimeOut check, Worker ID: %v, Task Reply: %+v\n", workID, taskReply)
					if time.Since(task.DispatchTime) > 10*time.Second {
						log.Printf("task %v is time out\n", task)
						c.workIdToTask.Delete(workID)
						switch task.Type {
						case 0:
							task.DispatchTime = time.Now()
							c.mapTask.Enqueue(task)
							log.Printf("reset map task %v, mapTask queue size: %d \n", task, c.mapTask.Len())
						case 1:
							task.DispatchTime = time.Now()
							c.reduceTask.Enqueue(task)
							log.Printf("reset reduce task %v, reduceTask queue size: %d \n", task, c.reduceTask.Len())
						}
					}
					return true
				})
			}
		}
	}()

	return &c
}
