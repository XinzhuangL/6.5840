package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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

// 传入自己的WorkerId 由Coordinator分配 Map 或 Reduce
type TaskArgs struct {
	WorkerID string
}

type TaskReply struct {
	Type               int       //任务类型 Map 或者 Reduce 0: Map 1: Reduce -1 FinishedFlag -2 nil
	TaskID             int       // valid type=0 or 1
	BucketNums         int       // 输出文件个数，即分配给几个Reduce Map使用  valid type = 0
	InputFileNames     []string  // 输入文件名称，分给每个map的文件数 供Map读取  valid type = 0 暂时为1
	TotalInputFileNums int       // 总输入文件数x reduce需要读取 output-x-reduceID的文件 valid type = 1
	DispatchTime       time.Time // 分发时间
}

// call back worker 回调
type CallBackArgs struct {
	WorkerID string
	TaskID   int
	Status   int // success 0 failed -1
}
type CallBackReply struct {
	Status int // success 0  failed -1 调用成功还是失败
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
