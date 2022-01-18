package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"time"
	"flag"
	"os"
	"strconv"
    "math/rand"
)

var cs corfs.FileSystem
var err error

var worker_id int
var job_id string
var object_size uint64
var object_data []byte
var bucket_path string
var operation string
var starttime, endtime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64
var write_time, read_time, read_same_file_time, delete_time float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	fmt.Println("S3 benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&worker_id, "id", 0, "Worker id")
	myflag.StringVar(&job_id, "jobId", "", "Job id")
	myflag.StringVar(&bucket_path, "b", "", "Bucket name")
	myflag.StringVar(&operation, "o", "all", "Operarion: w - write, r - read, rsf - readSameFile, d - delete")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	if worker_id == 0 {
		log.Fatal("Missing argument -id for worker id.")
	}

	if job_id == "" {
		log.Fatal("Missing argument -jobId for job id.")
	}

	if bucket_path == "" {
		log.Fatal("Missing argument -b for S3 bucket path.")
	}

	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Creates and initializes new S3 "filesystem"
	cs, err = corfs.InitFilesystem(1)
	if err != nil {
		log.Fatal("failed to create cache S3:",err)
	}

	// Run the write case
	if(operation == "w") {
		// Generate random byte array
		object_data = RandBytesRmndr(object_size)
		starttime = time.Now()
		runWrite(worker_id)
		
		write_time = float64(write_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / write_time
		logit(fmt.Sprintf("Worker %d WRITE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, write_time, bps), "s3_benchmark_"+job_id+".log")
	}

	// Run the read case
	if(operation == "r") {
		starttime = time.Now()
		runRead(worker_id)
		
		read_time := float64(read_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_time
		logit(fmt.Sprintf("Worker %d READ time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_time, bps), "s3_benchmark_"+job_id+".log")
	}
	
	// Run the read from same file case
	if(operation == "rsf") {
		starttime = time.Now()
		runReadSameFile(worker_id)
		
		read_same_file_time = float64(read_same_file_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_same_file_time
		logit(fmt.Sprintf("Worker %d READ SAME FILE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_same_file_time, bps), "s3_benchmark_"+job_id+".log")
	}

	// Run the delete case
	if(operation == "d") {
		starttime = time.Now()
		runDelete(worker_id)
		
		delete_time = float64(delete_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / delete_time
		logit(fmt.Sprintf("Worker %d DELETE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, delete_time, bps), "s3_benchmark_"+job_id+".log")
	}

}

func runWrite(worker_id int) {
	filename := "file" +  strconv.Itoa(worker_id) + ".out"
	log.Infof("Worker_%d START TIME OpenWriter: %+v", worker_id, time.Now())
	writer, err := cs.OpenWriter(cs.Join(bucket_path, filename))
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
			log.Errorf("Failed to write: %#v", err)
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
	reader, err := cs.OpenReader(cs.Join(bucket_path, filename), 0)
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
	reader, err := cs.OpenReader(cs.Join(bucket_path, filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	log.Infof("Worker_%d START TIME OpenReader: %+v", worker_id, time.Now())
	_, err = reader.Read(dataRead)
	
	defer func() {
		read_same_file_finish = time.Now()
		log.Infof("Worker_%d END TIME reading single file request: %+v", worker_id, read_same_file_finish)
	}()
}

func runDelete(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	log.Infof("Worker_%d START TIME delete: %+v", worker_id, time.Now())
	err := cs.Delete(cs.Join(bucket_path, filename))
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}
	
	defer func() {
		delete_finish = time.Now()
		log.Infof("Worker_%d END TIME delete request: %+v", worker_id, delete_finish)

	}()
}

func logit(msg string, fileName string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format("Mon 2006-01-2 17:06:04.000000") + ": " + msg + "\n")
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