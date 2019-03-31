package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)


type kvSortByKey []KeyValue

func (a kvSortByKey) Len() int {
	return len(a)
}
func (a kvSortByKey) Swap(i, j int){
	a[i], a[j] = a[j], a[i]
}
func (a kvSortByKey) Less(i, j int) bool {
	return a[j].Key < a[i].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// read json data from files
	var kvPairs []KeyValue
	for i := 0; i < nMap; i++ {
		file, error := os.Open(reduceName(jobName, i, reduceTask))
		if error != nil{
			log.Fatal("doReduce : openFile", error)
		}

		var kv KeyValue
		decode := json.NewDecoder(file)
		for{
			error_decode := decode.Decode(&kv)
			if error_decode == nil {
				break
			}
			kvPairs = append(kvPairs, kv)
		}
		file.Close()
	}

	// sort keyValue Pairs by Key
	sort.Sort(kvSortByKey(kvPairs))

	var preKey string = kvPairs[0].Key
	var values []string
	var outputKV []KeyValue
	for i := 0; i < len(kvPairs); i++{
		if kvPairs[i].Key == preKey{
			values = append(values, kvPairs[i].Value)
			if i == (len(kvPairs) - 1){
				outputKV = append(outputKV, KeyValue{preKey, reduceF(preKey, values)})
			}
		}else{
			outputKV = append(outputKV, KeyValue{preKey, reduceF(preKey, values)})
			values = make([]string, 0)
			values = append(values, kvPairs[i].Value)
			preKey = kvPairs[i].Key
		}
	}

	// write json into output file
	file, error := os.Create(outFile)
	if error != nil{
		log.Fatal("doReduce : writeFile", error)
	}

	encoder := json.NewEncoder(file)
	for _,kvPair := range outputKV{
		error_encode := encoder.Encode(&kvPair)
		if error_encode != nil{
			log.Fatal("doReduce : encode", error_encode)
		}
	}

	file.Close()

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
