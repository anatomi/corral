// Create EC2 instance 

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
)

var cs *corcache.DynamoCache
var err error

var duration_secs, threads, loops int
var object_size uint64
var table_name, partition_key, sort_key, value_attr string
var running_threads, write_count, read_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var endtime, write_finish, read_finish time.Time


func main() {
	fmt.Println("EFS benchmarking")
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.StringVar(&table_name, "table", "", "Table name")
	myflag.StringVar(&partition_key, "pk", "", "Partition key")
	myflag.StringVar(&sort_key, "sk", "", "Sort key")
	myflag.StringVar(&value_attr, "va", "", "Value attribute")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
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
	write_time := write_finish.Sub(starttime).Seconds()

	bps := float64(uint64(write_count)*object_size) / write_time
	logit(fmt.Sprintf("WRITE time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
		write_time, write_count, bytefmt.ByteSize(uint64(bps)), float64(write_count)/write_time, 0))


	// Run the read case
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
	read_time := read_finish.Sub(starttime).Seconds()

	bps = float64(uint64(read_count)*object_size) / read_time
	logit(fmt.Sprintf("READ time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
		read_time, read_count, bytefmt.ByteSize(uint64(bps)), float64(read_count)/read_time, 0))
}

func runWrite(thread_num int) {
	for time.Now().Before(endtime) {
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
		defer writer.Close()
	}
	write_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runRead(thread_num int) {
	for time.Now().Before(endtime) {
		atomic.AddInt32(&read_count, 1)
		filename := "file" + strconv.Itoa(thread_num) + ".out"
		reader, err := cs.OpenReader((filename), 0)
		if err != nil {
			log.Errorf("failed to open reader, %+v", err)
		}
		var dataRead []byte
		_, err = reader.Read(dataRead)
	}
	read_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}