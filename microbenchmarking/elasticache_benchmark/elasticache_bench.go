package main

import (
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"code.cloudfoundry.org/bytefmt"
	"sync/atomic"
	"time"
	"flag"
	"os"
	"net/http"
	"strconv"
)

var cs *corcache.RedisBackedCache
var err error

var duration_secs, threads, loops int
var object_size uint64
var redis_addrs string
var operation string
var running_threads, write_count, read_count, read_same_file_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var starttime, endtime, write_finish, read_finish, read_same_file_finish time.Time
var bps float64


func main() {
	fmt.Println("Elasticache + Redis benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.StringVar(&redis_addrs, "a", "", "Cluster endpoint with host:port")
	myflag.StringVar(&operation, "o", "all", "Operarion: w write, r read, rsf readSameFile, all ")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	if redis_addrs == "" {
		log.Fatal("Missing argument -a for redis cluster endpoint.")
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
	if(operation == "w" || operation == "all") {
		running_threads = int32(threads)
		starttime := time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runWrite(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		write_time := write_finish.Sub(starttime).Milliseconds()
		
		bps := float64(uint64(write_count)*object_size) / float64(write_time)
		logit(fmt.Sprintf("WRITE time %.5f msecs, objects = %d, speed = %sB/msec, %.1f operations/msec. Slowdowns = %d",
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

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_time := read_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_count)*object_size) / float64(read_time)
		logit(fmt.Sprintf("READ time %.5f msecs, objects = %d, speed = %sB/msec, %.1f operations/msec. Slowdowns = %d",
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

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		read_same_file_time := read_same_file_finish.Sub(starttime).Milliseconds()

		bps = float64(uint64(read_same_file_count)*object_size) / float64(read_same_file_time)
		logit(fmt.Sprintf("READ SAME FILE time %.5f msecs, objects = %d, speed = %sB/msec, %.1f operations/msec. Slowdowns = %d",
		float64(read_same_file_time), read_same_file_count, bytefmt.ByteSize(uint64(bps)), float64(read_same_file_count)/float64(read_same_file_time), 0))
	}
}

func runWrite(thread_num int) {
	atomic.AddInt32(&write_count, 1)
	// writing data to filesystem
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	writer, err := cs.OpenWriter(filename)
	if err != nil {
		log.Errorf("failed to open writer, %+v",err)
	}

	object_size := 10
	// generate "zero" byte array of given size
	data := make([]byte, object_size)
	// rand.Read(data)

	_, err = writer.Write(data)
	defer func() {
		err = writer.Close()
		if err != nil{
			log.Infof("Err: %#v", err)
		}

		defer func() {
			write_finish = time.Now()
			// One less thread
			atomic.AddInt32(&running_threads, -1)
		}()

	}()
}

func runRead(thread_num int) {
	// reading data from filesystem
	atomic.AddInt32(&read_count, 1)
	filename := "file" + strconv.Itoa(thread_num) + ".out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	_, err = reader.Read(dataRead)

	read_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runReadSameFile(thread_num int) {
	atomic.AddInt32(&read_same_file_count, 1)
	filename := "file" + strconv.Itoa(1) + ".out"
	reader, err := cs.OpenReader((filename), 0)
	if err != nil {
		log.Errorf("failed to open reader, %+v", err)
	}
	var dataRead []byte
	_, err = reader.Read(dataRead)
	
	read_same_file_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("elasticache_benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}