package corcache

import (
	"testing"
)

func TestRedisCacheSystem (t *testing.T) {
	rcs := &RedisBackedCache{
		DeploymentStrategy: &LocalRedisDeploymentStrategy{},
	}

	RunTestCacheSystem(t,rcs)
}