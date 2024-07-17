package mr

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

func hashToBuckets(s string, numBuckets int) int {
	bucket := ihash(s) % numBuckets
	return bucket
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// init a uuid
	workID := uuid.New().String()
	log.Printf("Create a new worker, ID: %d\n", workID)
	for {
		// wait coordinator init finished
		time.Sleep(1 * time.Second)
		reply := CallFetchTask(workID)
		if reply.Type >= 0 {
			ExecTask(reply, mapf, reducef)
			CallBackTask(workID, reply.TaskID, 0)
		} else if reply.Type == -1 {
			log.Printf("All Task is finished, break\n")
			break
		} else if reply.Type == -2 {
			log.Printf("Fetch null, All Task is running\n")
		} else {
			log.Fatalf("unknown task type: %d\n", reply.Type)
		}
	}
	// circle call fetch a task
	// exec

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ExecTask(reply *TaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	switch reply.Type {
	case 0:
		intermediate := make([][]KeyValue, reply.BucketNums)
		for i := range intermediate {
			intermediate[i] = []KeyValue{} // 初始化每个桶的切片
		}
		for _, filename := range reply.InputFileNames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				index := hashToBuckets(kv.Key, reply.BucketNums)
				intermediate[index] = append(intermediate[index], kv)
			}
		}
		for idx, kvs := range intermediate {
			sort.Sort(ByKey(kvs))
			oname := fmt.Sprintf("out-%v-%v", reply.TaskID, idx)
			ofile, _ := os.Create(oname)
			// todo compact
			for _, kv := range kvs {
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			}
			ofile.Close()
		}
		break
	case 1:
		oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
		ofile, _ := os.Create(oname)
		intermediate := []KeyValue{}
		// how may maps
		for k := 0; k < reply.TotalInputFileNums; k++ {
			iname := fmt.Sprintf("out-%v-%v", k, reply.TaskID)
			file, _ := os.Open(iname)
			defer file.Close()
			scanner := bufio.NewScanner(file)

			for scanner.Scan() {
				var kv KeyValue
				line := scanner.Text()
				_, err := fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
				if err != nil {
					log.Fatalf("cannot parse %v: %v", line, err)
				}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))
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
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		ofile.Close()
		break
	default:
		fmt.Printf("task type: %d\n", reply.Type)
	}

}

func CallFetchTask(workID string) *TaskReply {
	args := TaskArgs{WorkerID: workID}
	reply := TaskReply{}

	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}

}

func CallBackTask(workID string, taskID int, status int) *CallBackReply {
	args := CallBackArgs{
		WorkerID: workID,
		TaskID:   taskID,
		Status:   status,
	}
	reply := CallBackReply{}
	ok := call("Coordinator.CallBackTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
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
func call(rpcname string, args interface{}, reply interface{}) bool {
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
