package corlambda

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corbuild"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"strings"

	lambdaMessages "github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	log "github.com/sirupsen/logrus"
)

// MaxLambdaRetries is the number of times to try invoking a function
// before giving up and returning an error
const MaxLambdaRetries = 3

// LambdaClient wraps the AWS Lambda API and provides functions for
// deploying and invoking lambda functions
type LambdaClient struct {
	Client lambdaiface.LambdaAPI

}

type LambdaCacheConfigInjector interface {
	corcache.CacheConfigIncector
	ConfigureLambda(*lambda.CreateFunctionInput) error
}

// FunctionConfig holds the configuration of an individual Lambda function
type FunctionConfig struct {
	Name                string
	RoleARN             string
	Timeout             int64
	MemorySize          int64
	CacheConfigInjector corcache.CacheConfigIncector
}

// NewLambdaClient initializes a new LambdaClient
func NewLambdaClient() *LambdaClient {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return &LambdaClient{
		Client: lambda.New(sess),
	}
}

func functionNeedsUpdate(functionCode []byte, cfg *lambda.FunctionConfiguration) bool {
	codeHash := sha256.New()
	codeHash.Write(functionCode)
	codeHashDigest := base64.StdEncoding.EncodeToString(codeHash.Sum(nil))
	return codeHashDigest != *cfg.CodeSha256
}

func functionConfigNeedsUpdate(function *FunctionConfig, cfg *lambda.FunctionConfiguration) bool {
	return function.RoleARN != *cfg.Role || function.MemorySize != *cfg.MemorySize || function.Timeout != *cfg.Timeout
}

// updateFunctionSettings updates the provided lambda function settings (i.e. for memory and timeout)
func (l *LambdaClient) updateFunctionSettings(function *FunctionConfig) error {
	params := &lambda.UpdateFunctionConfigurationInput{
		FunctionName: aws.String(function.Name),
		Role:         aws.String(function.RoleARN),
		Timeout:      aws.Int64(function.Timeout),
		MemorySize:   aws.Int64(function.MemorySize),
	}
	_, err := l.Client.UpdateFunctionConfiguration(params)
	return err
}

// DeployFunction deploys the current directory as a lamba function
func (l *LambdaClient) DeployFunction(function *FunctionConfig) error {
	functionCode, _, err := corbuild.BuildPackage("main")
	if err != nil {
		panic(err)
	}

	exists, err := l.getFunction(function.Name)
	if exists != nil && err == nil {
		updated := false
		if functionNeedsUpdate(functionCode, exists.Configuration) {
			log.Infof("Updating Lambda function code for '%s'", function.Name)
			err = l.updateFunction(function, functionCode)
			updated = true
		}
		if functionConfigNeedsUpdate(function, exists.Configuration) {
			err = l.updateFunctionSettings(function)
			log.Infof("Updating Lambda function config for '%s'", function.Name)
			updated = true
		}
		if !updated {
			log.Infof("Function '%s' is already up-to-date", function.Name)
		}
		return err
	}

	log.Infof("Creating Lambda function '%s'", function.Name)
	return l.createFunction(function, functionCode)
}

// DeleteFunction tears down the given function
func (l *LambdaClient) DeleteFunction(functionName string) error {
	deleteInput := &lambda.DeleteFunctionInput{
		FunctionName: aws.String(functionName),
	}

	log.Debugf("Deleting function '%s'", functionName)
	_, err := l.Client.DeleteFunction(deleteInput)
	if err != nil && !strings.HasPrefix(err.Error(), lambda.ErrCodeResourceNotFoundException) {
		return err
	}
	return nil
}

// updateFunction updates the lambda function with the given name with the given code as function binary
func (l *LambdaClient) updateFunction(function *FunctionConfig, code []byte) error {
	updateArgs := &lambda.UpdateFunctionCodeInput{
		ZipFile:      code,
		FunctionName: aws.String(function.Name),
	}

	_, err := l.Client.UpdateFunctionCode(updateArgs)
	return err
}

// createFunction creates a lambda function with the given name and uses code as the function binary
func (l *LambdaClient) createFunction(function *FunctionConfig, code []byte) error {
	funcCode := &lambda.FunctionCode{
		ZipFile: code,
	}

	//injecting config values into deployment
	env := &lambda.Environment{}
	variables := make(map[string]*string)
	corbuild.InjectConfiguration(variables)
	env.SetVariables(variables)

	createArgs := &lambda.CreateFunctionInput{
		Code:         funcCode,
		FunctionName: aws.String(function.Name),
		Handler:      aws.String("main"),
		Runtime:      aws.String(lambda.RuntimeGo1X),
		Role:         aws.String(function.RoleARN),
		Timeout:      aws.Int64(function.Timeout),
		MemorySize:   aws.Int64(function.MemorySize),
		Environment:  env,
	}
	
	//we need to inject a cache config
	if function.CacheConfigInjector != nil {
		
		if li,ok := function.CacheConfigInjector.(LambdaCacheConfigInjector); ok {
			err := li.ConfigureLambda(createArgs)
			if err != nil{
				log.Warnf("failed to inject cache config into function")
				return err
			}
		} else {
			log.Errorf("cannot configure cache for this type of function, check the docs.")
			return fmt.Errorf("can't deploy function without injecting cache config")
		}
	}

	_, err := l.Client.CreateFunction(createArgs)
	return err
}

func (l *LambdaClient) getFunction(functionName string) (*lambda.GetFunctionOutput, error) {
	getInput := &lambda.GetFunctionInput{
		FunctionName: aws.String(functionName),
	}

	return l.Client.GetFunction(getInput)
}

type invokeError struct {
	Message    string                                           `json:"errorMessage"`
	StackTrace []lambdaMessages.InvokeResponse_Error_StackFrame `json:"stackTrace"`
}

func (l *LambdaClient) tryInvoke(functionName string, payload []byte) ([]byte, error) {
	invokeInput := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}

	output, err := l.Client.Invoke(invokeInput)
	if err != nil {
		return nil, err
	} else if output.FunctionError != nil {
		var errPayload invokeError
		err = json.Unmarshal(output.Payload, &errPayload)
		if err != nil {
			log.Debug(output.Payload)
			return nil, err
		}

		// Log stack trace if one was returned
		if len(errPayload.StackTrace) > 0 {
			log.Debug("Function invocation error. Stack trace:")
			for _, frame := range errPayload.StackTrace {
				log.Debugf("\t%s\t%s:%d", frame.Label, frame.Path, frame.Line)
			}
		}

		return output.Payload, fmt.Errorf("Function error: %s", errPayload.Message)
	}
	return output.Payload, err
}

// Invoke invokes the given Lambda function with the given payload.
func (l *LambdaClient) Invoke(functionName string, payload []byte) (outputPayload []byte, err error) {
	for try := 0; try < MaxLambdaRetries; try++ {
		outputPayload, err = l.tryInvoke(functionName, payload)
		if err == nil {
			break
		}
		log.Warnf("Function invocation failed. (Attempt %d of %d)", try+1, MaxLambdaRetries)
	}
	return outputPayload, err
}
