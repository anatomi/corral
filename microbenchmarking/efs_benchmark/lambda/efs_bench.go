package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"time"
	"strconv"
    "math/rand"
)

var cs *corcache.AWSEFSCache
var err error

var task_id int
var job_id string
var object_size uint64
var object_data []byte
var filesystem_path string
var operation string
var starttime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64
var write_time, read_time, read_same_file_time, delete_time float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
var permittedOperations = []string{"w", "r", "rsf", "d"}

type Event struct {
	TaskID int
	JobID string
	FilesystemPath string
	Operation string
	Filesize string
}

type Response struct {
	WriteTime float64
	ReadTime float64
	RSFTime float64
	DeleteTime float64
}

func HandleLambdaEvent(event Event) (Response, error) {
	fmt.Println("---------- EFS benchmarking ----------")
	bps = 0
	
	if event.FilesystemPath == "" {
		log.Fatal("Missing path to filesystem.")
	} else {
		filesystem_path = event.FilesystemPath
	}

	if event.TaskID == 0 {
		log.Fatal("Missing id of task.")
	} else {
		task_id = event.TaskID
	}

	if event.JobID == "" {
		log.Fatal("Missing id of job.")
	} else {
		job_id = event.JobID
	}

	if object_size, err = bytefmt.ToBytes(event.Filesize); err != nil {
		log.Fatalf("Invalid file size: %v", err)
	}

	if event.Operation == "" {
		log.Fatal("Missing operation parameter -o")
	} else {
		if (contains(permittedOperations, event.Operation)) {
			operation = event.Operation
		} else {
			log.Fatal("Invalid operation")
		}
	}

	
	cs, err = corcache.NewEFSCache()
	if err != nil {
		log.Fatal("failed to init EFS cache:", err)
	}

	log.Infof("------------------- Job: %s -------------------", job_id)
	log.Infof("Task config: id -  %d, filesize - %s, operation - %s", task_id, event.Filesize, operation)
	
	// Run the write case
	if(operation == "w") {
		// Generate random byte array
		object_data = RandBytesRmndr(object_size)
		starttime = time.Now()
		runWrite(task_id)

		write_time = float64(write_finish.Sub(starttime).Milliseconds())
		
		bps = float64(object_size) / write_time
		log.Infof("Task %d WRITE time %.5f msecs, speed = %.5fB/msec",
		task_id, write_time, bps)
	}	

	// Run the read case
	if(operation == "r") {
		starttime = time.Now()
		runRead(task_id)
		
		read_time = float64(read_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_time
		log.Infof("Task %d READ time %.5f msecs, speed = %.5fB/msec.",
		task_id, read_time, bps)
	}

	// Run the read from same file case
	if(operation == "rsf") {
		starttime = time.Now()
		runReadSameFile(task_id)
		
		read_same_file_time = float64(read_same_file_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_same_file_time
		log.Infof("Task %d READ SAME FILE time %.5f msecs, speed = %.5fB/msec.",
		task_id, read_same_file_time, bps)
	}

	// Run the delete case
	if(operation == "d") {
		starttime = time.Now()
		runDelete(task_id)

		delete_time = float64(delete_finish.Sub(starttime).Milliseconds())
		
		bps = float64(object_size) / delete_time
		log.Infof("Task %d DELETE time %.5f msecs, speed = %.5fB/msec.",
		task_id, delete_time, bps)
	}

	return  Response{WriteTime: write_time, ReadTime: read_time, RSFTime: read_same_file_time, DeleteTime: delete_time}, nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}

func runWrite(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	log.Infof("Worker_%d START TIME OpenWriter: %+v", worker_id, time.Now())
	writer, err := cs.OpenWriter(cs.Join(filesystem_path, filename))
	if err != nil {
		log.Errorf("failed to open writer, %+v",err)
	}

	log.Infof("Worker_%d START TIME Write: %+v", worker_id, time.Now())
	_, err = writer.Write(object_data)
	if err != nil {
		log.Fatal("Failed to write data,", err)
	}
	defer func() {
		err = writer.Close()
		if err != nil{
			log.Infof("Err: %#v", err)
		}

		defer func() {
			write_finish = time.Now()
			log.Infof("Worker_%d END TIME writing request: %+v", worker_id, write_finish)
		}()

	}()
}

func runRead(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	log.Infof("Worker_%d START TIME OpenReader: %+v", worker_id, time.Now())
	reader, err := cs.OpenReader(cs.Join(filesystem_path, filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	log.Infof("Worker_%d START TIME Read: %+v", worker_id, time.Now())
	_, err = reader.Read(dataRead)

	defer func() {
		read_finish = time.Now()
		log.Infof("Worker_%d END TIME reading request: %+v", worker_id, read_finish)
	}()
}

func runReadSameFile(worker_id int) {
	filename := "file1.out"
	log.Infof("Worker_%d START TIME OpenReader: %+v", worker_id, time.Now())
	reader, err := cs.OpenReader(cs.Join(filesystem_path, filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	log.Infof("Worker_%d START TIME Read same file: %+v", worker_id, time.Now())
	_, err = reader.Read(dataRead)
	
	defer func() {
		read_same_file_finish = time.Now()
		log.Infof("Worker_%d END TIME reading single file request: %+v", worker_id, read_same_file_finish)
	}()
}

func runDelete(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	log.Infof("Worker_%d START TIME delete: %+v", worker_id, time.Now())
	err := cs.Delete(cs.Join(filesystem_path, filename))
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}

	defer func() {
		delete_finish = time.Now()
		log.Infof("Worker_%d END TIME delete request: %+v", worker_id, delete_finish)

	}()
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