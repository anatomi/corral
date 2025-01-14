package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"time"
	"flag"
	"os"
	"strconv"
    "math/rand"
	"io"
)

var cs *corcache.DynamoCache
var err error

var worker_id int
var job_id string
var object_size uint64
var object_data []byte
var table_name, partition_key, sort_key, value_attr string
var operation string
var starttime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64
var write_time, read_time, read_same_file_time, delete_time float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	fmt.Println("DynamoDB benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&worker_id, "id", 0, "Worker id")
	myflag.StringVar(&job_id, "jobId", "", "Job id")
	myflag.StringVar(&operation, "o", "all", "Operarion: w - write, r - read, rsf - readSameFile, d - delete")
	myflag.StringVar(&table_name, "table", "", "Table name")
	myflag.StringVar(&partition_key, "pk", "", "Partition key")
	myflag.StringVar(&sort_key, "sk", "", "Sort key")
	myflag.StringVar(&value_attr, "va", "", "Value attribute")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1G", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	
	if worker_id == 0 {
		log.Fatal("Missing argument -id for worker id.")
	}

	if job_id == "" {
		log.Fatal("Missing argument -jobId for job id.")
	}

	if table_name == "" {
		log.Fatal("Missing argument -table for DynamoDB table name.")
	}

	if partition_key == "" {
		log.Fatal("Missing argument -pk for DynamoDB table partition key.")
	}

	if sort_key == "" {
		log.Fatal("Missing argument -pk for DynamoDB table sort key.")
	}

	if value_attr == "" {
		log.Fatal("Missing argument -va for DynamoDB table value attribute.")
	}

	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}
	
	cs, err = corcache.NewDynamoCache()
	if err != nil {
		log.Fatal("failed to init DynamoDB cache:",err)
	}

	// Init client
	cs.NewDynamoClient()

	cs.Config = &corcache.DynamoConfig{
		TableName: table_name,
		TablePartitionKey: partition_key,
		TableSortKey: sort_key,
		ValueAttribute: value_attr,
	}

	// Run the write case
	if(operation == "w") {
		// Generate random byte array
		object_data = RandBytesRmndr(object_size)
		starttime = time.Now()
		logit(fmt.Sprintf("Worker_%d WRITE_START_TIME %+v", worker_id, starttime), "dynamo_benchmark_"+job_id+".log")		
		runWrite(worker_id)
		
		write_time = float64(write_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / write_time
		logit(fmt.Sprintf("Worker_%d WRITE time %.5f msecs, speed = %.5f B/msec.",
		worker_id, write_time, bps), "dynamo_benchmark_"+job_id+".log")
	}
	
	// Run the read case
	if(operation == "r") {
		starttime = time.Now()
		logit(fmt.Sprintf("Worker_%d READ_START_TIME %+v", worker_id, starttime), "dynamo_benchmark_"+job_id+".log")
		runRead(worker_id)
		
		read_time := float64(read_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_time
		logit(fmt.Sprintf("Worker_%d READ time %.5f msecs, speed = %.5f B/msec.",
		worker_id, read_time, bps), "dynamo_benchmark_"+job_id+".log")
	}

	// Run the read from same file case
	if(operation == "rsf") {
		starttime = time.Now()
		logit(fmt.Sprintf("Worker_%d RSF_START_TIME: %+v", worker_id, starttime), "dynamo_benchmark_"+job_id+".log")
		runReadSameFile(worker_id)
		
		read_same_file_time = float64(read_same_file_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_same_file_time
		logit(fmt.Sprintf("Worker_%d RSF time %.5f msecs, speed = %.5f B/msec.",
		worker_id, read_same_file_time, bps), "dynamo_benchmark_"+job_id+".log")
	}

	// Run the delete case
	if(operation == "d") {
		starttime = time.Now()
		logit(fmt.Sprintf("Worker_%d DELETE_START_TIME %+v", worker_id, starttime), "dynamo_benchmark_"+job_id+".log")
		runDelete(worker_id)
		
		delete_time = float64(delete_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / delete_time
		logit(fmt.Sprintf("Worker_%d DELETE time %.5f msecs, speed = %.5f B/msec.",
		worker_id, delete_time, bps), "dynamo_benchmark_"+job_id+".log")
	}
	
}

func runWrite(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	writer, err := cs.OpenWriter(filename)
	if err != nil {
		log.Errorf("failed to open writer, %+v",err)
	}

	_, err = writer.Write(object_data)
	if err != nil {
		log.Fatal("Failed to write data,", err)
	}

	defer func() {
		err = writer.Close()
		if err != nil{
			log.Errorf("Failed to close writer: %#v", err)
		}

		defer func() {
			write_finish = time.Now()
			logit(fmt.Sprintf("Worker_%d WRITE_END_TIME %+v", worker_id, write_finish), "dynamo_benchmark_"+job_id+".log")
		}()

	}()
}

func runRead(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}

	buffer, err := io.ReadAll(reader)
	if err != nil {
		log.Errorf("failed to read file, %+v", err)
	}
	logit(fmt.Sprintf("Worker_%d READ_TEXT_LENGTH %d", worker_id, len(buffer)), "dynamo_benchmark_"+job_id+".log")

		
	defer func() {
		err = reader.Close()
		if err != nil {
			log.Errorf("failed to close reader, %+v", err)
		}
		read_finish = time.Now()
		logit(fmt.Sprintf("Worker_%d READ_END_TIME %+v", worker_id, read_finish), "dynamo_benchmark_"+job_id+".log")
	}()
}

func runReadSameFile(worker_id int) {
	filename := "file1.out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}

	buffer, err := io.ReadAll(reader)
	if err != nil {
		log.Errorf("failed to read file, %+v", err)
	}
	logit(fmt.Sprintf("Worker_%d READ_TEXT_LENGTH %d", worker_id, len(buffer)), "dynamo_benchmark_"+job_id+".log")

		
	defer func() {
		err = reader.Close()
		if err != nil {
			log.Errorf("failed to close reader, %+v", err)
		}
		read_same_file_finish = time.Now()
		logit(fmt.Sprintf("Worker_%d RSF_END_TIME %+v", worker_id, read_same_file_finish), "dynamo_benchmark_"+job_id+".log")
	}()
}

func runDelete(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	err := cs.Delete(filename)
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}
	
	defer func() {
		delete_finish = time.Now()
		logit(fmt.Sprintf("Worker_%d DELETE_END_TIME %+v", worker_id, delete_finish), "dynamo_benchmark_"+job_id+".log")
	}()
}

func logit(msg string, fileName string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(job_id + " " + time.Now().Format("Mon 2006-01-2 17:06:04.000000") + ": " + msg + "\n")
		logfile.Close()
	}
}

func RandBytesRmndr(n uint64) []byte {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
    }
    return b
}