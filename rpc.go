package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

type ReceiveRequestFromWorker_args struct {
    Useless int
}

type ReceiveRequestFromWorker_replys struct {
	State, Reduce_num, Map_num, Current_map, Current_reduce int
	File_name string
	// reduce_files []string
}

type MapWorkerFinished_args struct {
	Map_idx int
}

type MapWorkerFinished_replys struct {
	Useless int
}

type ReduceWorkerFinished_args struct {
	Reduce_idx int
}

type ReduceWorkerFinished_replys struct {
	Useless int
}



// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
