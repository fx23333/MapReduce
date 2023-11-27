package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
    for {
		args := ReceiveRequestFromWorker_args{}
		reply := ReceiveRequestFromWorker_replys{}
		call_sucess := call("Master.ReceiveRequestFromWorker", &args, &reply)
		if (!call_sucess)	{
			return
		}
		//fmt.Printf("Received task: %v\n", reply.State)
		if (reply.State == 0) {
    		//map task
			filename := reply.File_name
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Worker/Map: cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Worker/Map: cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			//---- map finished ----


			// var reduce_filename [reply.Reduce_num]string
			// var reduce_files [reply.Reduce_num]*os.File
			// var enc [reply.Reduce_num]*json.Encoder
			reduce_filename := make([]string, reply.Reduce_num)
			reduce_files := make([]*os.File, reply.Reduce_num)
			enc := make([]*json.Encoder, reply.Reduce_num)
			

			//create files encoders
			for i := 0; i < reply.Reduce_num; i++ {
				reduce_filename[i] = "mr-" + strconv.Itoa(reply.Current_map) + "-" + strconv.Itoa(i)
				reduce_files[i], _ = os.Create(reduce_filename[i])
				// fmt.Println("filename:", reduce_filename[i])
				enc[i] = json.NewEncoder(reduce_files[i])
			}



			//partition process
			for idx, kv := range kva {
				reducer := ihash(kv.Key) % reply.Reduce_num
				// fmt.Printf("file:%v, key:%v\n", reducer, kva[idx].Key)
				enc[reducer].Encode(&kva[idx])
			}
			// enc := json.NewEncoder(file)
			// for _, kv := ... {
			//   err := enc.Encode(&kv)
			for i := 0; i < reply.Reduce_num; i++ {
				reduce_files[i].Close()
			}
			argsfinished := MapWorkerFinished_args{}
			argsfinished.Map_idx = reply.Current_map
			replyfinished := MapWorkerFinished_replys{}
			call("Master.MapWorkerFinished", &argsfinished, &replyfinished)
			//kva = append(kva, kva...)





		} else if (reply.State == 1) {
			//reduce task
			var kva []KeyValue
			for i := 0; i < reply.Map_num; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Current_reduce)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Worker/Reducer: cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))

			
			oname := "mr-out-" + strconv.Itoa(reply.Current_reduce)
			ofile, _ := os.Create(oname)


			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		
				i = j
			}
			ofile.Close()

			// --- reduce task finished ---
			argsfinished := ReduceWorkerFinished_args{}
			argsfinished.Reduce_idx = reply.Current_reduce
			replyfinished := ReduceWorkerFinished_replys{}
			call("Master.ReduceWorkerFinished", &argsfinished, &replyfinished)
			

		} else if (reply.State == 2) {
			//wait
			time.Sleep(time.Duration(30) * time.Millisecond)
		} else {
			return
		}
	}






	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
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
