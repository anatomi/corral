package corcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKubeConfig(t *testing.T){
	krds := KubernetesRedisDeploymentStrategy{
		StorageClass: "zfs",
		//NodePort:     corcache.IntOptional(30841),
	}

	conf,err := krds.config()

	if err != nil{
		t.Fatal(err)
	}

	assert.NotEmpty(t, conf)

	t.Log(conf)


}

func TestKubernetesDeployment(t *testing.T){
	//XXX we should mock this

	krds := KubernetesRedisDeploymentStrategy{
		StorageClass: "zfs",
		NodePort:     IntOptional(30841),
	}
	r,err := krds.Deploy()
	defer krds.Undeploy()

	if err != nil {
		t.Fatal(err)
	}

	rcs := &RedisBackedCache{
		DeploymentStragey: &krds,
		Config:            r,
	}



	err = rcs.Init()

	CacheSmokeTest(t,rcs)

	if err != nil {
		t.Fatal(err)
	}



}
