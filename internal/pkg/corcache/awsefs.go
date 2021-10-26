package corcache

import (
	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/aws/aws-sdk-go/service/lambda"
	"io"
)

type AWSEFSCache struct {

}

func (A *AWSEFSCache) ListFiles(path string) ([]corfs.FileInfo, error) {
	panic("implement me")
}

func (A *AWSEFSCache) Stat(path string) (corfs.FileInfo, error) {
	panic("implement me")
}

func (A *AWSEFSCache) OpenReader(path string, startAt int64) (io.ReadCloser, error) {
	panic("implement me")
}

func (A *AWSEFSCache) OpenWriter(path string) (io.WriteCloser, error) {
	panic("implement me")
}

func (A *AWSEFSCache) Delete(path string) error {
	panic("implement me")
}

func (A *AWSEFSCache) Join(elem ...string) string {
	panic("implement me")
}

func (A *AWSEFSCache) Split(path string) []string {
	panic("implement me")
}

func (A *AWSEFSCache) Init() error {
	panic("implement me")
}

func (A *AWSEFSCache) Deploy() error {
	panic("implement me")
}

func (A *AWSEFSCache) Undeploy() error {
	panic("implement me")
}

func (A *AWSEFSCache) Flush(fs corfs.FileSystem) error {
	panic("implement me")
}

func (A *AWSEFSCache) Clear() error {
	panic("implement me")
}

func (A *AWSEFSCache) FunctionInjector() CacheConfigIncector {
	return &AWSEFSCacheConfigInjector{A}
}

type AWSEFSCacheConfigInjector struct {
	system *AWSEFSCache
}

func (a *AWSEFSCacheConfigInjector) CacheSystem() CacheSystem {
	return a.system
}

func (a *AWSEFSCacheConfigInjector) ConfigureLambda(functionConfig *lambda.CreateFunctionInput) error {
	panic("implement me")
}
