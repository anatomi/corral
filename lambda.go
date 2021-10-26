package corral

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/anatomi/corral/internal/pkg/coriam"
	"github.com/anatomi/corral/internal/pkg/corlambda"
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

func lambdaHostID() string {
	if lambdaNodeName != "" {
		return lambdaNodeName
	}

	f, err := os.Open("/proc/self/cgroup")
	defer f.Close()
	if err == nil {
		reader := bufio.NewScanner(f)
		for reader.Scan() {
			line := reader.Text()
			if idx:=strings.Index(line,"sandbox-root-");idx>=0 {
				line = line[idx:]
				lambdaNodeName = line[13:19]
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
	result, err := handle(lambdaDriver, lambdaHostID,func() string{

		if lc, ok := lambdacontext.FromContext(ctx); ok {
			rId := lc.AwsRequestID
			sn := lambdacontext.LogStreamName
			return fmt.Sprintf("%s_%s",rId,sn)
		}

		return os.Getenv("AWS_LAMBDA_LOG_STREAM_NAME")
	})(task)
	if err != nil {
		return "", err
	}

	payload, _ := json.Marshal(result)
	return string(payload), nil

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

func (l *lambdaExecutor) HintSplits(splits uint) error {
	return nil
}

func (l *lambdaExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.FilesystemType(job.fileSystem),
		CacheSystemType:  corcache.CacheSystemTypes(job.cacheSystem),
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
		CacheSystemType:  corcache.CacheSystemTypes(job.cacheSystem),
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

func (l *lambdaExecutor) Deploy(driver *Driver) error {
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

	if driver.cache != nil {
		config.CacheConfigInjector = driver.cache.FunctionInjector()
	}

	return l.DeployFunction(config)
}

func (l *lambdaExecutor) Undeploy() error {
	log.Info("Undeploying function")

	err := l.LambdaClient.DeleteFunction(l.functionName)
	if err != nil {
		log.Errorf("Error when undeploying function: %s", err)
	}

	log.Info("Undeploying IAM Permissions")
	iam_err := l.IAMClient.DeletePermissions(corralRoleName)
	if iam_err != nil {
		log.Errorf("Error when undeploying IAM permissions: %s", err)
		if err != nil{
			err = fmt.Errorf("Error when undeploying %s, %s",err,iam_err)
		} else {
			err = iam_err
		}
	}

	return err

}
