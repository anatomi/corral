package corcache

import (
	"testing"
)

func TestRedisCacheSystem (t *testing.T) {
	rcs := &RedisBackedCache{
		DeploymentStragey: &LocalRedisDeploymentStrategy{},
	}

	RunTestCacheSystem(t,rcs)
}