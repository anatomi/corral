package corral

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corwhisk"
	"io"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
)

var (
	whiskDriver *Driver
)

// runningInLambda infers if the program is running in AWS lambda via inspection of the environment
func runningInWhisk() bool {
	expectedEnvVars := []string{"__OW_API_KEY", "__OW_API_HOST"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

//TODO: prob. need to lift this into its own interface/abstraction
func handleWhsikRequest(task task) (taskResult, error) {
	// Precaution to avoid running out of memory for reused Lambdas
	debug.FreeOSMemory()

	// Setup current job
	fs := corfs.InitFilesystem(task.FileSystemType)
	currentJob := lambdaDriver.jobs[task.JobNumber]
	currentJob.fileSystem = fs
	currentJob.intermediateBins = task.IntermediateBins
	currentJob.outputPath = task.WorkingLocation
	currentJob.config.Cleanup = task.Cleanup

	// Need to reset job counters in case this is a reused lambda
	currentJob.bytesRead = 0
	currentJob.bytesWritten = 0

	if task.Phase == MapPhase {
		err := currentJob.runMapper(task.BinID, task.Splits)
		result := taskResult{
			BytesRead:    int(currentJob.bytesRead),
			BytesWritten: int(currentJob.bytesWritten),
		}
		return result, err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(task.BinID)
		result := taskResult{
			BytesRead:    int(currentJob.bytesRead),
			BytesWritten: int(currentJob.bytesWritten),
		}
		return result, err
	}
	return taskResult{}, fmt.Errorf("Unknown phase: %d", task.Phase)
}

type whiskExecutor struct {
	*corwhisk.WhiskClient
	functionName string
}

func newWhiskExecutor(functionName string) *whiskExecutor {
	return &whiskExecutor{
		WhiskClient:  corwhisk.NewWhiskClient(),
		functionName: functionName,
	}
}

//Implement the action loop that we trigger in the runtime
//this implements a process execution using system in and out...
//this is a modified version of https://github.com/apache/openwhisk-runtime-go/blob/master/examples/standalone/exec.go
func loop() {
	// debugging
	var debug = os.Getenv("OW_DEBUG") != ""

	if debug {
		filename := os.Getenv("OW_DEBUG")
		f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err == nil {
			log.SetOutput(f)
			defer f.Close()
		}
		log.Printf("ACTION ENV: %v", os.Environ())
	}

	// assign the main function
	type Action func(event task) (taskResult, error)
	var action Action
	action = handleWhsikRequest

	// input
	out := os.NewFile(3, "pipe")
	defer out.Close()
	reader := bufio.NewReader(os.Stdin)

	// read-eval-print loop
	if debug {
		log.Println("started")
	}
	// send ack
	// note that it depends on the runtime,
	// go 1.13+ requires an ack, past versions does not
	fmt.Fprintf(out, `{ "ok": true}%s`, "\n")
	for {
		// read one line
		inbuf, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		if debug {
			log.Printf(">>>'%s'>>>", inbuf)
		}
		// parse one line
		var input map[string]interface{}
		err = json.Unmarshal(inbuf, &input)
		if err != nil {
			log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}
		if debug {
			log.Printf("%v\n", input)
		}
		// set environment variables
		err = json.Unmarshal(inbuf, &input)
		for k, v := range input {
			if k == "value" {
				continue
			}
			if s, ok := v.(string); ok {
				os.Setenv("__OW_"+strings.ToUpper(k), s)
			}
		}
		// get payload if not empty
		var payload task
		if value, ok := input["value"].(map[string]interface{}); ok {
			//Ya, I know uggly but this is simpler than manual parsing of the map[]interface
			buffer, _ := json.Marshal(value)
			_ = json.Unmarshal(buffer, &payload)
		}
		// process the request
		result, err := action(payload)
		if err != nil {
			log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}
		// encode the answer
		output, err := json.Marshal(&result)
		if err != nil {
			log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}
		output = bytes.Replace(output, []byte("\n"), []byte(""), -1)
		if debug {
			log.Printf("'<<<%s'<<<", output)
		}
		fmt.Fprintf(out, "%s\n", output)
	}
}

func (l *whiskExecutor) Start(d *Driver) {
	whiskDriver = d
	loop()
}

func prepareWhiskResult(payload io.ReadCloser) taskResult {
	var result taskResult
	data, err := ioutil.ReadAll(payload)
	if err != nil {
		log.Errorf("%s", err)
	}

	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Errorf("%s", err)
	}
	return result
}

func (l *whiskExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.S3,
		WorkingLocation:  job.outputPath,
	}

	resp, err := l.Invoke(l.functionName, mapTask)
	if err != nil {
		log.Warnf("invocation failed with err:%+v", err)
		return err
	}

	taskResult := prepareWhiskResult(resp)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *whiskExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	mapTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.S3,
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
	}
	resp, err := l.Invoke(l.functionName, mapTask)
	if err != nil {
		log.Warnf("invocation failed with err:%+v", err)
		return err
	}

	taskResult := prepareWhiskResult(resp)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *whiskExecutor) Deploy() {
	conf := corwhisk.WhiskFunctionConfig{
		Memory:  viper.GetInt("lambdaMemory"),
		Timeout: viper.GetInt("lambdaTimeout"),
	}

	err := l.WhiskClient.DeployFunction(conf)

	if err != nil {
		log.Infof("failed to deploy %s - %+v", l.functionName, err)
	}

}

func (l *whiskExecutor) Undeploy() {
	err := l.WhiskClient.DeleteFunction(l.functionName)
	if err != nil {
		log.Infof("failed to remove function %+v", err)
	}
}
