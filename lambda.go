package corral

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
	"github.com/ISE-SMILE/corral/internal/pkg/coriam"
	"github.com/ISE-SMILE/corral/internal/pkg/corlambda"
)

var (
	lambdaDriver *Driver
)

// corralRoleName is the name to use when deploying an IAM role
const corralRoleName = "CorralExecutionRole"

// runningInLambda infers if the program is running in AWS lambda via inspection of the environment
func runningInLambda() bool {
	expectedEnvVars := []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

var lambdaNodeName string

func lambdaHostID() string{
	if lambdaNodeName != ""{
		return lambdaNodeName
	}

	f,err := os.Open("/proc/self/cgroup")
	defer f.Close()
	if err == nil {
		reader := bufio.NewScanner(f)
		for reader.Scan(){
			line := reader.Text()
			if strings.Contains(line,"cpu") {
				lambdaNodeName = line
				return lambdaNodeName
			}
		}
	}

	uptime := readUptime()
	if uptime != "" {
		lambdaNodeName = uptime
		return lambdaNodeName
	}

	lambdaNodeName = "unknown"
	return lambdaNodeName
}

func handleRequest(ctx context.Context, task task) (string, error) {
	result,err := handle(lambdaDriver,lambdaHostID)(task)
	if err != nil{
		return "",err
	} else {
		payload, _ := json.Marshal(result)
		return string(payload),nil
	}
}

type lambdaExecutor struct {
	*corlambda.LambdaClient
	*coriam.IAMClient
	functionName string
}

func newLambdaExecutor(functionName string) *lambdaExecutor {
	return &lambdaExecutor{
		LambdaClient: corlambda.NewLambdaClient(),
		IAMClient:    coriam.NewIAMClient(),
		functionName: functionName,
	}
}

func loadTaskResult(payload []byte) taskResult {
	// Unescape JSON string
	payloadStr, _ := strconv.Unquote(string(payload))

	var result taskResult
	err := json.Unmarshal([]byte(payloadStr), &result)
	if err != nil {
		log.Errorf("%s", err)
	}
	return result
}

func (l *lambdaExecutor) Start(d *Driver) {
	lambdaDriver = d
	d.Start = time.Now()
	log.Info("called Start")
	lambda.Start(handleRequest)
}

func (l *lambdaExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.FilesystemType(job.fileSystem),
		WorkingLocation:  job.outputPath,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.functionName, payload)
	taskResult := loadTaskResult(resultPayload)

	job.collectActivation(taskResult)
	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *lambdaExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	mapTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.FilesystemType(job.fileSystem),
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := l.Invoke(l.functionName, payload)
	taskResult := loadTaskResult(resultPayload)

	job.collectActivation(taskResult)
	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (l *lambdaExecutor) Deploy() {
	var roleARN string
	var err error
	if viper.GetBool("lambdaManageRole") {
		roleARN, err = l.DeployPermissions(corralRoleName)
		if err != nil {
			panic(err)
		}
	} else {
		roleARN = viper.GetString("lambdaRoleARN")
	}

	config := &corlambda.FunctionConfig{
		Name:       l.functionName,
		RoleARN:    roleARN,
		Timeout:    viper.GetInt64("lambdaTimeout"),
		MemorySize: viper.GetInt64("lambdaMemory"),
	}
	err = l.DeployFunction(config)
	if err != nil {
		panic(err)
	}
}

func (l *lambdaExecutor) Undeploy() {
	log.Info("Undeploying function")
	err := l.LambdaClient.DeleteFunction(l.functionName)
	if err != nil {
		log.Errorf("Error when undeploying function: %s", err)
	}

	log.Info("Undeploying IAM Permissions")
	err = l.IAMClient.DeletePermissions(corralRoleName)
	if err != nil {
		log.Errorf("Error when undeploying IAM permissions: %s", err)
	}
}
