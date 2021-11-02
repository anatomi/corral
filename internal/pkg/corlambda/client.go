package corlambda

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corbuild"
	"github.com/ISE-SMILE/corral/internal/pkg/corcache"
	"github.com/aws/aws-sdk-go/service/s3"
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

//Wrapping Package generation and deployment-prepration into an interface for mocking and extension
type FunctionDeployment interface {
	Package() error
	NeedsUpdate(cfg *lambda.FunctionConfiguration) bool
	Prepare() (*lambda.FunctionCode, error)
}

// FunctionConfig holds the configuration of an individual Lambda function
type FunctionConfig struct {
	FunctionDeployment
	Name                string
	RoleARN             string
	Timeout             int64
	MemorySize          int64
	CacheConfigInjector corcache.CacheConfigIncector

	code     []byte
	S3Key    string
	S3Bucket string
	CodeHash string
}

func (d *FunctionConfig) hash() string {
	codeHash := sha256.New()
	codeHash.Write(d.code)
	codeHashDigest := base64.StdEncoding.EncodeToString(codeHash.Sum(nil))
	d.CodeHash = codeHashDigest
	return d.CodeHash
}

func (d *FunctionConfig) NeedsUpdate(cfg *lambda.FunctionConfiguration) bool {
	if d.FunctionDeployment != nil {
		return d.FunctionDeployment.NeedsUpdate(cfg)
	}

	if d.CodeHash == "" {
		d.hash()
	}

	return d.CodeHash != *cfg.CodeSha256
}

func (d *FunctionConfig) Prepare() (*lambda.FunctionCode, error) {
	if d.FunctionDeployment != nil {
		return d.FunctionDeployment.Prepare()
	}

	if d.S3Key != "" && d.S3Bucket != "" {
		log.Infof("Uploading deployment package to S3 %s/%s", d.S3Bucket, d.S3Key)
		sess, err := session.NewSession()
		if err != nil {
			return nil, err
		}
		s3Client := s3.New(sess)

		req, _ := s3Client.PutObjectRequest(&s3.PutObjectInput{
			Body:   bytes.NewReader(d.code),
			Bucket: &d.S3Bucket,
			Key:    &d.S3Key,
		})

		err = req.Send()
		if err != nil {
			log.Errorf("failed to upload deployment to S3 %e", err)
			return nil, err
		}

		return &lambda.FunctionCode{
			S3Bucket: &d.S3Bucket,
			S3Key:    &d.S3Bucket,
		}, nil
	} else {
		log.Infof("Uploading deployment as zip")
		return &lambda.FunctionCode{
			ZipFile: d.code,
		}, nil
	}

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

func (d *FunctionConfig) Package() error {
	if d.FunctionDeployment != nil {
		return d.FunctionDeployment.Package()
	}

	code, _, err := corbuild.BuildPackage("main")
	d.code = code
	return err
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

	//TODO: we might need to apply cache changes, how do we check for that?

	_, err := l.Client.UpdateFunctionConfiguration(params)
	return err
}

// DeployFunction deploys the current directory as a lamba function
func (l *LambdaClient) DeployFunction(function *FunctionConfig) error {
	err := function.Package()
	if err != nil {
		return err
	}

	exists, err := l.getFunction(function.Name)
	if exists != nil && err == nil {
		updated := false
		if function.NeedsUpdate(exists.Configuration) {
			log.Infof("Updating Lambda function code for '%s'", function.Name)
			err = l.updateFunction(function)
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
	return l.createFunction(function)
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
func (l *LambdaClient) updateFunction(function *FunctionConfig) error {
	funcCode, err := function.Prepare()
	if err != nil {
		return err
	}
	updateArgs := &lambda.UpdateFunctionCodeInput{
		FunctionName:    aws.String(function.Name),
		ImageUri:        funcCode.ImageUri,
		S3Bucket:        funcCode.S3Bucket,
		S3Key:           funcCode.S3Key,
		S3ObjectVersion: funcCode.S3ObjectVersion,
		ZipFile:         funcCode.ZipFile,
	}

	_, err = l.Client.UpdateFunctionCode(updateArgs)
	//XXX: should we delete the deployment zip if this fails?
	return err
}

// createFunction creates a lambda function with the given name and uses code as the function binary
func (l *LambdaClient) createFunction(function *FunctionConfig) error {
	funcCode, err := function.Prepare()
	if err != nil {
		return err
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

		if li, ok := function.CacheConfigInjector.(LambdaCacheConfigInjector); ok {
			err := li.ConfigureLambda(createArgs)
			if err != nil {
				log.Warnf("failed to inject cache config into function")
				return err
			}
		} else {
			log.Errorf("cannot configure cache for this type of function, check the docs.")
			return fmt.Errorf("can't deploy function without injecting cache config")
		}
	}

	_, err = l.Client.CreateFunction(createArgs)
	//XXX: should we delete the deployment zip if this fails?
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
