package corcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestElasticacheConfig(t *testing.T){
	NumNodes := int64(2)
	ecrds := ElasticacheRedisDeploymentStrategy{
		NodeType:      	"cache.t2.micro",//viper.GetString("elasticacheNodeType"),
		NumCacheNodes: 	&NumNodes,
		EngineVersion:  "6.x",//viper.GetString("elasticacheEngineVersion"),
	}

	conf,err := ecrds.config()

	if err != nil{
		t.Fatal(err)
	}

	assert.NotEmpty(t, conf)

	t.Log(conf)
}

func TestElasticacheDeployment(t *testing.T){
	NumNodes := int64(2)
	ecrds := ElasticacheRedisDeploymentStrategy{
		NodeType:      	"cache.t2.micro",//viper.GetString("elasticacheNodeType"),
		NumCacheNodes: 	&NumNodes,
		EngineVersion:  "6.x",//viper.GetString("elasticacheEngineVersion"),
	}
	deploy,err := ecrds.Deploy()
	defer ecrds.Undeploy()

	if err != nil {
		t.Fatal(err)
	}

	rcs := &RedisBackedCache{
		DeploymentStrategy: &ecrds,
		Config:            deploy,
	}

	
	err = rcs.Init()

	CacheSmokeTest(t,rcs)

	if err != nil {
		t.Fatal(err)
	}


}
