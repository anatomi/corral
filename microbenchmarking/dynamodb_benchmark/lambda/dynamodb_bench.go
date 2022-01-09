package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"sync/atomic"
	"time"
	"strconv"
    "math/rand"
)

var cs *corcache.DynamoCache
var err error

var threads int
var object_size uint64
var object_data []byte
var table_name, partition_key, sort_key, value_attr string
var operation string
var running_threads, write_count, read_count, read_same_file_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var starttime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
var permittedOperations = []string{"w", "r", "rsf", "d", "all"}

type Event struct {
	Threads int
	TableName string
	TablePK string
	TableSK string
	TableValueAttr string
	Operation string
	Filesize string
}

func HandleLambdaEvent(event Event) (error) {
	fmt.Println("DynamoDB benchmarking")
	running_threads, write_count, read_count, read_same_file_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count = 0, 0, 0, 0, 0, 0, 0, 0 
	bps = 0

	if event.TableName == "" {
		log.Fatal("Missing table name.")
	} else {
		table_name = event.TableName
	}

	if event.TablePK == "" {
		log.Fatal("Missing table partition key.")
	} else {
		partition_key = event.TablePK
	}

	if event.TableSK == "" {
		log.Fatal("Missing table sort key.")
	} else {
		sort_key = event.TableSK
	}

	if event.TableValueAttr  == "" {
		log.Fatal("Missing table value attribute.")
	} else {
		value_attr = event.TableValueAttr
	}

	if event.Threads == 0 {
		log.Fatal("Missing number of threads.")
	} else {
		threads = event.Threads
	}

	if object_size, err = bytefmt.ToBytes(event.Filesize); err != nil {
		log.Fatalf("Invalid file size: %v", err)
	}
	log.Infof("Size: %d", object_size)

	if event.Operation == "" {
		operation = "all"
	} else {
		if (contains(permittedOperations, event.Operation)) {
			operation = event.Operation
		} else {
			log.Fatal("Invalid operation")
		}
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

	log.Infof("Benchark config: threads -  %d, filesize - %s, operation - %s", threads, event.Filesize, operation)

	// Run the write case
	if(operation == "w" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		for n := 1; n <= threads; n++ {
			go runWrite(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		write_time := write_finish.Sub(starttime).Milliseconds()
		
		bps := float64(uint64(write_count)*object_size) / float64(write_time)
		log.Infof("WRITE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec.",
		float64(write_time), write_count, bytefmt.ByteSize(uint64(bps)), float64(write_count)/float64(write_time))
	}
	
	// Run the read case
	if(operation == "r" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		for n := 1; n <= threads; n++ {
			go runRead(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_time := read_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_count)*object_size) / float64(read_time)
		log.Infof("READ time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec.",
		float64(read_time), read_count, bytefmt.ByteSize(uint64(bps)), float64(read_count)/float64(read_time))
	}

	// Run the read from same file case
	if(operation == "rsf" || operation == "all") {
		running_threads = int32(threads)
		starttime = time.Now()
		for n := 1; n <= threads; n++ {
			go runReadSameFile(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_same_file_time := read_same_file_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_same_file_count)*object_size) / float64(read_same_file_time)
		log.Infof("READ SAME FILE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec.",
		float64(read_same_file_time), read_same_file_count, bytefmt.ByteSize(uint64(bps)), float64(read_same_file_count)/float64(read_same_file_time))
	}

	// Run the delete case
	if(operation == "d" || operation == "all") {
		running_threads = int32(threads)
		starttime := time.Now()
		for n := 1; n <= threads; n++ {
			go runDelete(n)
		}

		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		delete_time := delete_finish.Sub(starttime).Milliseconds()
		
		bps := float64(uint64(delete_count)*object_size) / float64(delete_time)
		log.Infof("DELETE time %.5f msecs, objects = %d, speed = %sB/msec, %.5f operations/msec.",
		float64(delete_time), delete_count, bytefmt.ByteSize(uint64(bps)), float64(delete_count)/float64(delete_time))
	}
	return nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
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

func RandBytesRmndr(n uint64) []byte {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
    }
    return b
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}