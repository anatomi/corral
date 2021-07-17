package corral

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
	"github.com/ISE-SMILE/corral/internal/pkg/corlambda"

	"github.com/stretchr/testify/assert"
)

func TestRunningInLambda(t *testing.T) {
	res := runningInLambda()
	assert.False(t, res)

	for _, env := range []string{"LAMBDA_TASK_ROOT", "AWS_EXECUTION_ENV", "LAMBDA_RUNTIME_DIR"} {
		os.Setenv(env, "value")
	}

	res = runningInLambda()
	assert.True(t, res)
}

func TestHandleRequest(t *testing.T) {
	testTask := task{
		JobNumber:        0,
		Phase:            MapPhase,
		BinID:            0,
		IntermediateBins: 10,
		Splits:           []inputSplit{},
		FileSystemType:   corfs.Local,
		WorkingLocation:  ".",
	}

	job := &Job{
		config: &config{},
	}

	// These values should be reset to 0 by Lambda handler function
	job.bytesRead = 10
	job.bytesWritten = 20

	lambdaDriver = NewDriver(job)
	lambdaDriver.runtimeID = "foo"
	lambdaDriver.Start = time.Time{}

	mockTaskResult := taskResult{
		BytesRead:    0,
		BytesWritten: 0,
		Log:          "",
		HId:          mockHostID(),
		RId:          mockHostID(),
		CId:          lambdaDriver.runtimeID,
		JId:          "0_0",//phase_bin
		CStart:       lambdaDriver.Start.UnixNano(),
		EStart:       0,
		EEnd:         0,
	}

	output, err := handle(lambdaDriver,mockHostID,mockHostID)(testTask)
	assert.Nil(t, err)
	output.EStart = 0
	output.EEnd = 0
	assert.Equal(t, mockTaskResult, output)

	testTask.Phase = ReducePhase
	mockTaskResult.JId = "1_0"
	output, err = handle(lambdaDriver,mockHostID,mockHostID)(testTask)

	assert.Nil(t, err)
	output.EStart = 0
	output.EEnd = 0
	assert.Equal(t, mockTaskResult, output)
}

func mockHostID() string {
	return "test"
}

type mockLambdaClient struct {
	lambdaiface.LambdaAPI
	capturedPayload []byte
}

func (m *mockLambdaClient) Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
	m.capturedPayload = input.Payload
	return &lambda.InvokeOutput{}, nil
}

func (*mockLambdaClient) GetFunction(*lambda.GetFunctionInput) (*lambda.GetFunctionOutput, error) {
	return nil, nil
}

func (*mockLambdaClient) CreateFunction(*lambda.CreateFunctionInput) (*lambda.FunctionConfiguration, error) {
	return nil, nil
}

func TestRunLambdaMapper(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	job := &Job{
		config: &config{WorkingLocation: "."},
	}
	err := executor.RunMapper(job, 0, 10, []inputSplit{})
	assert.Nil(t, err)

	var taskPayload task
	err = json.Unmarshal(mock.capturedPayload, &taskPayload)
	assert.Nil(t, err)

	assert.Equal(t, uint(10), taskPayload.BinID)
	assert.Equal(t, MapPhase, taskPayload.Phase)
}

func TestRunLambdaReducer(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	job := &Job{
		config: &config{WorkingLocation: "."},
	}
	err := executor.RunReducer(job, 0, 10)
	assert.Nil(t, err)

	var taskPayload task
	err = json.Unmarshal(mock.capturedPayload, &taskPayload)
	assert.Nil(t, err)

	assert.Equal(t, uint(10), taskPayload.BinID)
	assert.Equal(t, ReducePhase, taskPayload.Phase)
}

func TestDeployFunction(t *testing.T) {
	mock := &mockLambdaClient{}
	executor := &lambdaExecutor{
		&corlambda.LambdaClient{
			Client: mock,
		},
		nil,
		"FunctionName",
	}

	viper.SetDefault("lambdaManageRole", false) // Disable testing role deployment
	executor.Deploy()
}
