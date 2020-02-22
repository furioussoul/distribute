package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type InterFile struct {
	enc  *json.Encoder
	file *os.File
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	fmt.Println("worker started !")
	var lastTask *Task = nil
	for {
		select {
		case <-time.After(1000000000):
			request := PollArgs{}
			reply := PollReply{}

			if lastTask != nil {
				request.LastTask = *lastTask
			}

			fmt.Printf("last task %+v \n", request.LastTask)

			ok := call("Master.Poll", &request, &reply)

			lastTask = &reply.Task

			if !ok {
				fmt.Printf("Poll failed")
				continue
			}

			if reply.Done {
				fmt.Printf("all tasks have been done \n")
				return
			}

			if reply.Pending {
				fmt.Printf("all tasks pending\n")
				continue
			}

			if reply.Task.TaskType == 1 {
				fmt.Printf("start map task %+v \n", reply.Task)
				err := doMap(mapf, reply)
				if err != nil {
					log.Fatal(err)
				}

			} else if reply.Task.TaskType == 2 {
				fmt.Printf("start reduce task %+v \n", reply.Task)
				err := doReduce(reducef, reply)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, reply PollReply) error {
	mfile := reply.Task.Files[0]
	file, err := os.Open(mfile)
	if err != nil {
		log.Fatalf("cannot open %v", mfile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mfile)
	}
	file.Close()

	kva := mapf(mfile, string(content))

	encMap := make(map[int]InterFile)

	for i := 0; i < reply.Task.ReduceNum; i++ {
		ifile, _ := ioutil.TempFile("", "mr-inter-")
		encMap[i] = InterFile{
			enc:  json.NewEncoder(ifile),
			file: ifile,
		}
	}

	for i := range kva {
		kv := kva[i]
		err := encMap[ihash(kv.Key)%reply.Task.ReduceNum].enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i, file := range encMap {
		os.Rename(file.file.Name(), fmtInterFile(mfile, i))
		file.file.Close()
	}

	return nil
}

func fmtInterFile(file string, reduceId int) string {
	return fmt.Sprintf("%s-intermediate-%d", file, reduceId)
}

func doReduce(reducef func(string, []string) string, reply PollReply) error {

	files := reply.Task.Files

	reduceMap := make(map[string][]string)

	for i := range files {

		rfile := fmtInterFile(files[i], reply.Task.ReduceId)
		ofile, err := os.Open(rfile)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(ofile)
		for {
			kv := KeyValue{}
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			vals := reduceMap[kv.Key]
			reduceMap[kv.Key] = append(vals, kv.Value)
		}
	}

	ofile, _ := ioutil.TempFile("", "mr-out-")
	for k, v := range reduceMap {
		r := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, r)
	}
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reply.Task.ReduceId))

	return nil
}

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
