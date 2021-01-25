package corral

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"

	"github.com/ISE-SMILE/corral/internal/pkg/corwhisk"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
)

var (
	whiskDriver *Driver
)

func runningInWhisk() bool {
	expectedEnvVars := []string{"__OW_EXECUTION_ENV", "__OW_API_HOST"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

var whiskNodeName string

func whiskHostID() string{
	if whiskNodeName != "" {
		return whiskNodeName
	}
	//there are two possible scenarios: docker direct or kubernetes...

	//is only present in some upstream openwhisk builds ;)
	if _,err := os.Stat("/vmhost"); err == nil {
		data,err := ioutil.ReadFile("/vmhost")
		if err == nil{
			whiskNodeName = string(bytes.Replace(data,[]byte("\n"),[]byte(""),-1))
			return whiskNodeName
		}
	}
	if name := os.Getenv("NODENAME");name != ""{
		whiskNodeName = name
		return whiskNodeName
	}

	uptime := readUptime()
	if uptime != "" {
		whiskNodeName = uptime
		return whiskNodeName
	}

	whiskNodeName = "unknown"
	return whiskNodeName
}

func handleWhsikRequest(task task) (taskResult, error) {
	return handle(whiskDriver,whiskHostID)(task)
}

type whiskExecutor struct {
	corwhisk.WhiskClientApi
	functionName string
}

func newWhiskExecutor(functionName string) *whiskExecutor {
	return &whiskExecutor{
		WhiskClientApi:  corwhisk.NewWhiskClient(),
		functionName: functionName,
	}
}

//Implement the action loop that we trigger in the runtime
//this implements a process execution using system in and out...
//this is a modified version of https://github.com/apache/openwhisk-runtime-go/blob/master/examples/standalone/exec.go
func loop() {
	var logBuffer bytes.Buffer
	logFile := io.Writer(&logBuffer)
	log.SetOutput(logFile)
	//log.Printf("ACTION ENV: %v", os.Environ())


	// assign the main function
	type Action func(event task) (taskResult, error)
	var action Action
	action = handleWhsikRequest

	// input
	out := os.NewFile(3, "pipe")
	defer out.Close()
	reader := bufio.NewReader(os.Stdin)

	// read-eval-print loop
	//log.Println("started")

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

		log.Printf(">>>'%s'>>>", inbuf)

		// parse one line
		var input map[string]interface{}
		err = json.Unmarshal(inbuf, &input)
		if err != nil {
			log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}

		log.Printf("%v\n", input)

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
		var invocation corwhisk.WhiskPayload
		var payload task
		if value, ok := input["value"].(map[string]interface{}); ok {
			buffer, _ := json.Marshal(value)
			err = json.Unmarshal(buffer, &invocation)
			if err != nil{
				log.Println(err.Error())
				fmt.Fprintf(out, "{ error: %q}\n", err.Error())
				continue
			}
			buffer, _ = json.Marshal(invocation.Value)
			err = json.Unmarshal(buffer, &payload)
			if err != nil{
				log.Println(err.Error())
				fmt.Fprintf(out, "{ error: %q}\n", err.Error())
				continue
			}
			for k,v := range invocation.Env {
				if v != nil{
					os.Setenv("__OW_"+strings.ToUpper(k), *v)
				}
			}

		}
		// process the request
		result, err := action(payload)

		if err != nil {
			log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}
		if log.IsLevelEnabled(log.DebugLevel) {
			result.Log = logBuffer.String()
		}
		// encode the answer
		output, err := json.Marshal(&result)
		if err != nil {
			//log.Println(err.Error())
			fmt.Fprintf(out, "{ error: %q}\n", err.Error())
			continue
		}
		output = bytes.Replace(output, []byte("\n"), []byte(""), -1)

		log.Printf("'<<<%s'<<<", output)
		f, err := os.OpenFile("/tmp/activation.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err == nil {
			f.Write([]byte(result.Log))
			f.Close()
		}
		fmt.Fprintf(out, "%s\n", output)

	}
}

func (l *whiskExecutor) Start(d *Driver) {
	whiskDriver = d
	for {
		//can't stop won't stop
		loop()
	}
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
		FileSystemType:   corfs.FilesystemType(job.fileSystem),
		WorkingLocation:  job.outputPath,
	}

	resp, err := l.Invoke(l.functionName, mapTask)
	if err != nil {
		log.Warnf("invocation failed with err:%+v", err)
		return err
	}

	taskResult := prepareWhiskResult(resp)
	job.collectActivation(taskResult)
	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *whiskExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	mapTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.FilesystemType(job.fileSystem),
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
	}
	resp, err := l.Invoke(l.functionName, mapTask)
	if err != nil {
		log.Warnf("invocation failed with err:%+v", err)
		return err
	}

	taskResult := prepareWhiskResult(resp)
	job.collectActivation(taskResult)
	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *whiskExecutor) Deploy() {
	conf := corwhisk.WhiskFunctionConfig{
		FunctionName: l.functionName,
		Memory:       viper.GetInt("lambdaMemory"),
		Timeout:      viper.GetInt("lambdaTimeout") * 1000,
	}

	err := l.WhiskClientApi.DeployFunction(conf)

	if err != nil {
		log.Infof("failed to deploy %s - %+v", l.functionName, err)
	}

}

func (l *whiskExecutor) Undeploy() {
	err := l.WhiskClientApi.DeleteFunction(l.functionName)
	if err != nil {
		log.Infof("failed to remove function %+v", err)
	}
}
