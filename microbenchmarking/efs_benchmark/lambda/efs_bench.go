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

var worker_id int
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
	WorkerID int
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

	if event.WorkerID == 0 {
		log.Fatal("Missing id of worker.")
	} else {
		worker_id = event.WorkerID
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
	log.Infof("Worker config: id -  %d, filesize - %s, operation - %s", worker_id, event.Filesize, operation)
	
	// Run the write case
	if(operation == "w") {
		// Generate random byte array
		object_data = RandBytesRmndr(object_size)
		starttime = time.Now()
		log.Infof("Worker_%d WRITE START TIME: %+v", worker_id, starttime)
		runWrite(worker_id)

		write_time = float64(write_finish.Sub(starttime).Milliseconds())
		
		bps = float64(object_size) / write_time
		log.Infof("Worker %d WRITE time %.5f msecs, speed = %.5fB/msec",
		worker_id, write_time, bps)
	}	

	// Run the read case
	if(operation == "r") {
		starttime = time.Now()
		log.Infof("Worker_%d READ START TIME: %+v", worker_id, starttime)
		runRead(worker_id)
		
		read_time = float64(read_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_time
		log.Infof("Worker %d READ time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_time, bps)
	}

	// Run the read from same file case
	if(operation == "rsf") {
		starttime = time.Now()
		log.Infof("Worker_%d READ SAME FILE START TIME: %+v", worker_id, starttime)
		runReadSameFile(worker_id)
		
		read_same_file_time = float64(read_same_file_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_same_file_time
		log.Infof("Worker %d READ SAME FILE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_same_file_time, bps)
	}

	// Run the delete case
	if(operation == "d") {
		starttime = time.Now()
		log.Infof("Worker_%d DELETE START TIME: %+v", worker_id, starttime)
		runDelete(worker_id)

		delete_time = float64(delete_finish.Sub(starttime).Milliseconds())
		
		bps = float64(object_size) / delete_time
		log.Infof("Worker %d DELETE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, delete_time, bps)
	}

	return  Response{WriteTime: write_time, ReadTime: read_time, RSFTime: read_same_file_time, DeleteTime: delete_time}, nil
}

func main() {
	lambda.Start(HandleLambdaEvent)
}

func runWrite(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	writer, err := cs.OpenWriter(cs.Join(filesystem_path, filename))
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
			log.Infof("Worker_%d WRITE END TIME: %+v", worker_id, write_finish)
		}()

	}()
}

func runRead(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	reader, err := cs.OpenReader(cs.Join(filesystem_path, filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}

	var dataRead []byte
	_, err = reader.Read(dataRead)

	defer func() {
		err = reader.Close()
		if err != nil{
			log.Errorf("Failed to close reader: %#v", err)
		}

		defer func() {
			read_finish = time.Now()
			log.Infof("Worker_%d READ END TIME: %+v", worker_id, read_finish)
		}()
	}()
}

func runReadSameFile(worker_id int) {
	filename := "file1.out"
	reader, err := cs.OpenReader(cs.Join(filesystem_path, filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	_, err = reader.Read(dataRead)
	
	defer func() {
		err = reader.Close()
		if err != nil{
			log.Errorf("Failed to close reader: %#v", err)
		}

		defer func() {
			read_same_file_finish = time.Now()
			log.Infof("Worker_%d READ SAME FILE END TIME: %+v", worker_id, read_same_file_finish)
		}()
	}()
}

func runDelete(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	err := cs.Delete(cs.Join(filesystem_path, filename))
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}

	defer func() {
		delete_finish = time.Now()
		log.Infof("Worker_%d DELETE END TIME: %+v", worker_id, delete_finish)
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