package corral

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func loadConfig() {
	viper.SetConfigName("corralrc") //BUG .yaml?
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.corral")

	setupDefaults()

	err := viper.ReadInConfig()
	if err != nil {
		log.Debugf("Config Read %+v", err)
	}

	viper.SetEnvPrefix("corral")
	viper.AutomaticEnv()
}

func setupDefaults() {
	defaultSettings := map[string]interface{}{
		"lambdaFunctionName": "corral_function",
		"lambdaMemory":       1500,
		"lambdaTimeout":      180,
		"lambdaManageRole":   true,
		"lambdaS3Key":        "corral_code.zip",
		"lambdaS3Bucket":     "corral-code-bucket1",
		"cleanup":            false,
		"durable":            false, //Should Intermeidiate data be flushed to the filesystem (in conflict with cleanup)
		"verbose":            false,
		"splitSize":          100 * 1024 * 1024, // Default input split size is 100Mb
		"mapBinSize":         512 * 1024 * 1024, // Default map bin size is 512Mb
		"reduceBinSize":      512 * 1024 * 1024, // Default reduce bin size is 512Mb
		"maxConcurrency":     500,               // Maximum number of concurrent executors
		"workingLocation":    ".",
		"requestPerMinute":   200,
		"remoteLoggingHost":  "",
		"logName":            "activations",

		"cache": 4, //coresponse to corcache.CacheSystemType (0 - NoCache, 1 - Local, 2 - Redis, 3 - Olric, 4 - EFS, 5 - DynamoDB)

		"cacheSize": uint64(10 * 1024 * 1024), //corosponse to corcache.Local

		"redisDeploymentType":    	2,  //corosponse to corcache.Redisx
		"redisPort":              	nil,
		"redisClusterSize":			3,
		"kubernetesNamespace":    	"", //
		"kubernetesStorageClass": 	"",
		"elasticacheNodeType":    	"cache.t2.micro",
		"elasticacheEngineVersion": "6.x",
		

		// EFS config
		"efsFilesystemName": "corral_efs_filesystem",
		"efsAccessPointName": "corral_efs_accesspoint",
		"efsAccessPointPath": "/cache",
		"lambdaEfsPath": "/mnt/cache",

		// AWS VPC config
		"efsVPCSubnetIds": "", // List of subnet ids associated with VPC separated by ';'"
		"efsVPCSecurityGroupIds": "", // Security group id asociated with VPC, i.e.: "sg-085912345678*****"
	
		// DynamoDB config
		"dynamodbTableName": "CorralCache",

	}
	for key, value := range defaultSettings {
		viper.SetDefault(key, value)
	}

	aliases := map[string]string{
		"verbose":          "v",
		"working_location": "o",
		"lambdaMemory":     "m",
	}
	for key, alias := range aliases {
		viper.RegisterAlias(alias, key)
	}
}
