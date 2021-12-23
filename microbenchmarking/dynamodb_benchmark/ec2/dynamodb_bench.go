package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"sync/atomic"
	"time"
	"flag"
	"os"
	"net/http"
	"strconv"
    "math/rand"
)

var cs *corcache.DynamoCache
var err error

var duration_secs, threads, loops int
var object_size uint64
var object_data []byte
var table_name, partition_key, sort_key, value_attr string
var operation string
var running_threads, write_count, read_count, read_same_file_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var starttime, endtime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	fmt.Println("DynamoDB benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.StringVar(&operation, "o", "all", "Operarion: w - write, r - read, rsf - readSameFile, d - delete, all - all test cases")
	myflag.StringVar(&table_name, "table", "", "Table name")
	myflag.StringVar(&partition_key, "pk", "", "Partition key")
	myflag.StringVar(&sort_key, "sk", "", "Sort key")
	myflag.StringVar(&value_attr, "va", "", "Value attribute")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1G", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
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

	// Generate random byte array
	object_data = RandBytesRmndr(object_size)
	
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
	if(operation == "w" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runWrite(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		write_time := write_finish.Sub(starttime).Milliseconds()
		
		bps := float64(uint64(write_count)*object_size) / float64(write_time)
		logit(fmt.Sprintf("WRITE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec. Slowdowns = %d",
		float64(write_time), write_count, bytefmt.ByteSize(uint64(bps)), float64(write_count)/float64(write_time), 0))
	}
	
	// Run the read case
	if(operation == "r" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runRead(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_time := read_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_count)*object_size) / float64(read_time)
		logit(fmt.Sprintf("READ time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec. Slowdowns = %d",
		float64(read_time), read_count, bytefmt.ByteSize(uint64(bps)), float64(read_count)/float64(read_time), 0))
	}

	// Run the read from same file case
	if(operation == "rsf" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runReadSameFile(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_same_file_time := read_same_file_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_same_file_count)*object_size) / float64(read_same_file_time)
		logit(fmt.Sprintf("READ SAME FILE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec. Slowdowns = %d",
		float64(read_same_file_time), read_same_file_count, bytefmt.ByteSize(uint64(bps)), float64(read_same_file_count)/float64(read_same_file_time), 0))
	}

	// Run the delete case
	if(operation == "d" || operation == "all") {
		running_threads = int32(threads)
		starttime := time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDelete(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		delete_time := delete_finish.Sub(starttime).Milliseconds()
		
		bps := float64(uint64(delete_count)*object_size) / float64(delete_time)
		logit(fmt.Sprintf("DELETE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec. Slowdowns = %d",
		float64(delete_time), delete_count, bytefmt.ByteSize(uint64(bps)), float64(delete_count)/float64(delete_time), 0))
	}
	
}

func runWrite(thread_num int) {
	atomic.AddInt32(&write_count, 1)
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	writer, err := cs.OpenWriter(filename)
	if err != nil {
		log.Errorf("failed to open writer, %+v",err)
	}

	_, err = writer.Write(object_data)
	defer func() {
		err = writer.Close()
		if err != nil{
			log.Errorf("Failed to write: %#v", err)
		}

		defer func() {
			write_finish = time.Now()
			// One less thread
			atomic.AddInt32(&running_threads, -1)
		}()

	}()
}

func runRead(thread_num int) {
	atomic.AddInt32(&read_count, 1)
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	_, err = reader.Read(dataRead)
	
	read_finish = time.Now()
	atomic.AddInt32(&running_threads, -1)
}

func runReadSameFile(thread_num int) {
	atomic.AddInt32(&read_same_file_count, 1)
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	_, err = reader.Read(dataRead)
	
	read_same_file_finish = time.Now()
	atomic.AddInt32(&running_threads, -1)
}

func runDelete(thread_num int) {
	atomic.AddInt32(&delete_count, 1)
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	err := cs.Delete(filename)
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}
	
	delete_finish = time.Now()
	atomic.AddInt32(&running_threads, -1)
}

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("dynamo_benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
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