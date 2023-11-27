package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
//import "fmt"
import "time"



type Master struct {
	// Your definitions here.
    map_input []string

	//0 for not allocated
	//1 for processing
	//2 for finished
    map_file_state, reduce_file_state []int
	// reduce_input [][]string
	// //二维数组，一级索引为reduce标号，[i][j]表示j号map任务提供给i号reduce的文件
	reduce_num, map_num int
	map_finished, reduce_finished bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (m *Master) ReceiveRequestFromWorker(args *ReceiveRequestFromWorker_args, reply *ReceiveRequestFromWorker_replys) error {
	//0 for map
	//1 for reduce
	//2 for wait
	//3 for return
	m.mu.Lock()
	if (m.map_finished && m.reduce_finished) {
		reply.State = 3
		return nil
	}
	map_finished_temp:= true
	if (!m.map_finished) {
		for idx, file := range m.map_input {
			// fmt.Printf("file: %v %v\n", idx, file)
			if (m.map_file_state[idx] == 0) {
				reply.Reduce_num = m.reduce_num
				reply.Map_num = m.map_num
				reply.Current_map = idx

				reply.State = 0
				reply.File_name = file
				m.map_file_state[idx] = 1
				m.mu.Unlock()
				return nil
			} else if (m.map_file_state[idx] == 1) {
				map_finished_temp = false
			}
		}
	}
	m.map_finished = map_finished_temp
	reduce_finished_temp := true
	if (m.map_finished) {
		for idx, state := range m.reduce_file_state {
			if (state == 0) {
				reply.Reduce_num = m.reduce_num
				reply.Map_num = m.map_num
				reply.Current_reduce = idx

				reply.State = 1
				// ReceiveRequestFromWorker_replys.reduce_files = m.reduce_input[idx]
				m.reduce_file_state[idx] = 1;
				m.mu.Unlock()
				return nil
			} else if (state == 1) {
				reduce_finished_temp = false
			}
		}
	} else {		
		//map have not finished all, reduce workers should wait
		reply.State = 2
		m.mu.Unlock()
		return nil
	}
	m.reduce_finished = reduce_finished_temp
	reply.State = 2
	m.mu.Unlock()
	return nil
}

func (m *Master) MapWorkerFinished(args *MapWorkerFinished_args, reply *MapWorkerFinished_replys) error {
	m.mu.Lock()
	m.map_file_state[args.Map_idx] = 2
	m.mu.Unlock()
	return nil
}

func (m *Master) ReduceWorkerFinished(args *ReduceWorkerFinished_args, reply *ReduceWorkerFinished_replys) error {
	m.mu.Lock()
	m.reduce_file_state[args.Reduce_idx] = 2
	m.mu.Unlock()
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.map_finished && m.reduce_finished

	// Your code here.



}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (m *Master) test() {
	for {
		time.Sleep(time.Duration(10) * time.Second)
		m.mu.Lock()
		for idx, state := range m.map_file_state {
			if (state == 1) {
				m.map_file_state[idx] = 0
			}
		}
		for idx, state := range m.reduce_file_state {
			if (state == 1) {
				m.reduce_file_state[idx] = 0
			}
		}
		m.mu.Unlock()
	}
}


func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// for idx, file := range files {
	// 	fmt.Println("file", idx, " ", file)
	// }
	// fmt.Println(files)
	m.map_input = append(m.map_input, files[:]...)
	m.reduce_num = nReduce
	m.map_num = len(files)
	m.map_file_state = make([]int, len(files))
	m.reduce_file_state = make([]int, nReduce)
	go m.test()
	m.server()
	return &m
}
