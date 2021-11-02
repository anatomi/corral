package corral

import (
	"encoding/json"
	"github.com/anatomi/corral/internal/pkg/corbuild"
	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/anatomi/corral/internal/pkg/corwhisk"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunningInWhisk(t *testing.T) {
	res := runningInWhisk()
	assert.False(t, res)

	for _, env := range []string{"__OW_EXECUTION_ENV", "__OW_API_HOST"} {
		os.Setenv(env, "value")
	}

	res = runningInWhisk()
	assert.True(t, res)
}

func TestHandleWhiskRequest(t *testing.T) {
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

	whiskDriver = NewDriver(job)
	whiskDriver.runtimeID = "foo"
	whiskDriver.Start = time.Time{}

	mockTaskResult := taskResult{
		BytesRead:    0,
		BytesWritten: 0,
		Log:          "",
		HId:          mockHostID(),
		RId:          mockHostID(),
		CId:          whiskDriver.runtimeID,
		JId:          "0_0_0", //phase_bin
		CStart:       whiskDriver.Start.UnixNano(),
		EStart:       0,
		EEnd:         0,
	}

	output, err := handle(whiskDriver, mockHostID, mockHostID)(testTask)
	assert.Nil(t, err)
	output.EStart = 0
	output.EEnd = 0
	assert.Equal(t, mockTaskResult, output)

	testTask.Phase = ReducePhase
	mockTaskResult.JId = "0_1_0"
	output, err = handle(whiskDriver, mockHostID, mockHostID)(testTask)

	assert.Nil(t, err)
	output.EStart = 0
	output.EEnd = 0
	assert.Equal(t, mockTaskResult, output)
}

func TestRunWhiskMapper(t *testing.T) {
	mock := &mockWhiskClient{}
	executor := &whiskExecutor{
		mock,
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

func TestRunWhiskReducer(t *testing.T) {
	mock := &mockWhiskClient{}
	executor := &whiskExecutor{
		mock,
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

func TestRunWhiskDeployFunction(t *testing.T) {
	mock := &mockWhiskClient{}
	executor := &whiskExecutor{
		mock,
		"FunctionName",
	}

	viper.SetDefault("lambdaManageRole", false) // Disable testing role deployment
	executor.Deploy(&Driver{})
}

type mockWhiskClient struct {
	capturedPayload []byte
}

func (m *mockWhiskClient) Invoke(name string, payload interface{}) (io.ReadCloser, error) {
	data, err := json.Marshal(payload)
	m.capturedPayload = data

	result := taskResult{}
	data, err = json.Marshal(result)

	return ioutil.NopCloser(strings.NewReader(string(data))), err

}

func (m *mockWhiskClient) DeployFunction(conf corwhisk.WhiskFunctionConfig) error {
	return nil
}

func (m mockWhiskClient) DeleteFunction(name string) error {
	return nil
}

const inputJson = "{\"action_name\":\"/guest/corral_test\",\"action_version\":\"0.0.34\",\"activation_id\":\"2e5cd3da27b14f4d9cd3da27b15f4dcb\",\"deadline\":\"1611327680624\",\"namespace\":\"guest\",\"transaction_id\":\"lLcUN9mi4f8PILYeFP5bWTLgvoBsiC6j\",\"value\":{\"env\":{\"MINIO_HOST\":\"http://130.149.158.143:32005\",\"MINIO_KEY\":\"smile2021\",\"MINIO_USER\":\"smile\",\"OW_DEBUG\":\"true\"},\"value\":{\"BinID\":1,\"Cleanup\":false,\"FileSystemType\":2,\"IntermediateBins\":1,\"JobNumber\":0,\"Phase\":0,\"Splits\":[{\"EndOffset\":112639,\"Filename\":\"s3://input/metamorphosis.txt\",\"StartOffset\":102400},{\"EndOffset\":122879,\"Filename\":\"s3://input/metamorphosis.txt\",\"StartOffset\":112640},{\"EndOffset\":133119,\"Filename\":\"s3://input/metamorphosis.txt\",\"StartOffset\":122880},{\"EndOffset\":139053,\"Filename\":\"s3://input/metamorphosis.txt\",\"StartOffset\":133120}],\"WorkingLocation\":\"minio://results\"}}}"

type test struct{}

func (w test) Map(key, value string, emitter Emitter) {
	log.Infof("k:%s v:%s", key, value)
	emitter.Emit(key, value)
}

func (w test) Reduce(key string, values ValueIterator, emitter Emitter) {
	emitter.Emit(key, "")
}

func TestWiskLocalRuntime(t *testing.T) {
	if os.Getenv("__OW_MOCK_RUNTIME") == "" {
		t.SkipNow()
	}
	//start local minio?

	os.Setenv("__OW_EXECUTION_ENV", "foo")
	os.Setenv("__OW_API_HOST", "bar")

	job := NewJob(test{}, test{})
	driver := NewDriver(job, WithInputs("minio://input/*.txt"))

	viper.Set("verbose", true)
	//viper.Set("minioHost","http://130.149.158.143:32005")
	//viper.Set("minioUser", "smile")
	//viper.Set("minioKey","smile2021")
	//
	//flag.Set("backend","whisk")

	mapTask := task{
		JobNumber:        0,
		Phase:            MapPhase,
		BinID:            0,
		Splits:           []inputSplit{},
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.MINIO,
		WorkingLocation:  job.outputPath,
	}

	invocation := corwhisk.WhiskPayload{
		Value: mapTask,
		Env:   make(map[string]*string),
	}

	corbuild.InjectConfiguration(invocation.Env)
	strBool := "true"
	invocation.Env["OW_DEBUG"] = &strBool

	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Failed()
	}
	f.Write([]byte(inputJson))
	f.Write([]byte("\n"))
	f.Close()

	f, err = os.Open(f.Name())
	if err != nil {
		t.Failed()
	}
	os.Stdin = f

	driver.Main()
}
