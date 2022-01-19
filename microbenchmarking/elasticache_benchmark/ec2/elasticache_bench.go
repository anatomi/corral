package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"time"
	"flag"
	"os"
	"strconv"
    "math/rand"
)

var cs *corcache.RedisBackedCache
var err error

var worker_id int
var job_id string
var object_size uint64
var object_data []byte
var redis_addrs string
var operation string
var starttime, endtime, write_finish, read_finish, delete_finish, read_same_file_finish time.Time
var bps float64
var write_time, read_time, read_same_file_time, delete_time float64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	fmt.Println("Elasticache + Redis benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&worker_id, "id", 0, "Worker id")
	myflag.StringVar(&job_id, "jobId", "", "Job id")
	myflag.StringVar(&redis_addrs, "a", "", "Cluster endpoint with host:port")
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

	if redis_addrs == "" {
		log.Fatal("Missing argument -a for redis cluster endpoint.")
	}

	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}
	
	cs,err = corcache.NewRedisBackedCache(corcache.DeploymentType(2))
	if err != nil {
		log.Fatal("failed to init redis cache:",err)
	}

	// Init client
	cs.Client = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{redis_addrs}, // as flag
		//RouteByLatency: rc.RouteByLatency, // as flag
		//RouteRandomly:  rc.RouteRandomly, // as flag
	})


	// Run the write case
	if(operation == "w") {
		// Generate random byte array
		object_data = RandBytesRmndr(object_size)
		starttime = time.Now()
		log.Infof("Worker_%d WRITE START TIME: %+v", worker_id, starttime)
		runWrite(worker_id)
		
		write_time = float64(write_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / write_time
		logit(fmt.Sprintf("Worker %d WRITE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, write_time, bps), "elasticache_benchmark_"+job_id+".log")
	}

	// Run the read case
	if(operation == "r") {
		starttime = time.Now()
		log.Infof("Worker_%d READ START TIME: %+v", worker_id, starttime)
		runRead(worker_id)
		
		read_time := float64(read_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_time
		logit(fmt.Sprintf("Worker %d READ time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_time, bps), "elasticache_benchmark_"+job_id+".log")
	}

	// Run the read from same file case
	if(operation == "rsf") {
		starttime = time.Now()
		log.Infof("Worker_%d READ SAME FILE START TIME: %+v", worker_id, starttime)
		runReadSameFile(worker_id)
		
		read_same_file_time = float64(read_same_file_finish.Sub(starttime).Milliseconds())

		bps = float64(object_size) / read_same_file_time
		logit(fmt.Sprintf("Worker %d READ SAME FILE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, read_same_file_time, bps), "elasticache_benchmark_"+job_id+".log")
	}

	// Run the delete case
	if(operation == "d") {
		starttime = time.Now()
		log.Infof("Worker_%d DELETE START TIME: %+v", worker_id, starttime)
		runDelete(worker_id)
		
		delete_time = float64(delete_finish.Sub(starttime).Milliseconds())
		
		bps := float64(object_size) / delete_time
		logit(fmt.Sprintf("Worker %d DELETE time %.5f msecs, speed = %.5fB/msec.",
		worker_id, delete_time, bps), "elasticache_benchmark_"+job_id+".log")
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
			log.Infof("Worker_%d WRITE END TIME: %+v", worker_id, write_finish)
		}()

	}()
}

func runRead(worker_id int) {
	filename := "file" + strconv.Itoa(worker_id) + ".out"
	reader, err := cs.OpenReader((filename), 0)
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
	reader, err := cs.OpenReader((filename), 0)
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
	err := cs.Delete(filename)
	if err != nil {
		log.Errorf("failed to delete file, %+v", err)
	}
	
	defer func() {
		delete_finish = time.Now()
		log.Infof("Worker_%d DELETE END TIME: %+v", worker_id, delete_finish)
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