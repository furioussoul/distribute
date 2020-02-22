package mr

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapNum             int
	reduceNum          int
	workers            []string
	mapTasksPool       *list.List
	pendingMapTasks    map[string]int64
	reduceTasksPool    *list.List
	pendingReduceTasks map[int]int64
	files              []string
	workerDone         int
	done               bool
	stop               chan struct{}
	lock               sync.Mutex
}

func (m *Master) Poll(args *PollArgs, reply *PollReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.done {
		reply.Done = true
		return nil
	}

	if args.LastTask.TaskType == 1 {
		fmt.Printf("delete map %v \n", args.LastTask.Files[0])
		delete(m.pendingMapTasks, args.LastTask.Files[0])
	} else if args.LastTask.TaskType == 2 {
		fmt.Printf("delete reduce %v \n", args.LastTask.ReduceId)
		delete(m.pendingReduceTasks, args.LastTask.ReduceId)
	}
	if m.mapTasksPool.Len() > 0 {
		fmt.Printf("poll map %v \n", m.mapTasksPool.Len())
		task := m.mapTasksPool.Front()
		reply.Task.TaskType = 1
		reply.Task.ReduceNum = m.reduceNum
		file := task.Value.(string)
		reply.Task.Files = []string{file}
		m.mapTasksPool.Remove(task)
		m.pendingMapTasks[file] = time.Now().Unix()
		return nil
	} else if len(m.pendingMapTasks) > 0 {
		reply.Pending = true
		return nil
	} else if m.reduceTasksPool.Len() > 0 {
		fmt.Printf("poll reduce %+v \n", m.reduceTasksPool.Len())
		task := m.reduceTasksPool.Front()
		reply.Task.TaskType = 2
		reply.Task.ReduceId = task.Value.(int)
		reply.Task.ReduceNum = m.reduceNum
		reply.Task.Files = m.files
		m.reduceTasksPool.Remove(task)
		m.pendingReduceTasks[reply.Task.ReduceId] = time.Now().Unix()
		return nil
	} else if len(m.pendingReduceTasks) > 0 {
		reply.Pending = true
		return nil
	} else {
		log.Println("all tasks done, should shutdown in few seconds")
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	fmt.Println(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go m.schedule()
}

func (m *Master) isDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	done := m.mapTasksPool.Len() == 0 && len(m.pendingMapTasks) == 0 && m.reduceTasksPool.Len() == 0 && len(m.pendingReduceTasks) == 0

	return done
}

func (m *Master) schedule() {
	fmt.Println("schedule start")
	for {
		select {
		case <-time.After(1000999999):
			m.lock.Lock()
			//worker fail tolerance,worker may suddenly crash
			//there is no data recovery in worker, if worker crashed, we just push back task,
			//a new worker will redo the task,for inspecting worker crashing, simply monitor time
			//that task already spent
			now := time.Now().Unix()
			for k, v := range m.pendingMapTasks {
				if now-v > 10 {
					m.mapTasksPool.PushBack(k)
					delete(m.pendingMapTasks, k)
				}
			}

			for k, v := range m.pendingReduceTasks {
				if now-v > 10 {
					m.reduceTasksPool.PushBack(k)
					delete(m.pendingReduceTasks, k)
				}
			}
			m.lock.Unlock()

		case <-m.stop:
			return
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	done := m.isDone()
	if done {
		close(m.stop)
	}

	fmt.Println(done)

	return done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapNum = len(files)
	m.reduceNum = nReduce
	m.mapTasksPool = list.New()
	m.reduceTasksPool = list.New()
	m.pendingMapTasks = make(map[string]int64)
	m.pendingReduceTasks = make(map[int]int64)
	m.files = files
	m.stop = make(chan struct{})

	for i := range files {
		fmt.Println(files[i])
		m.mapTasksPool.PushBack(files[i])
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasksPool.PushBack(i)
	}

	m.server()

	return &m
}
