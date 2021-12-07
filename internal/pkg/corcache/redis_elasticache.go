package corcache

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/aws/aws-sdk-go/aws/awserr"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"fmt"
	"strings"
)

const ElasticacheClusterName = "corral-redis-cluster-disabled"
const ElasticacheClusterSubnetGroupName = "corral-cluster-subnet-group"

type ElasticacheRedisDeploymentStrategy struct {
	NodeType string
	NodePort *int64 
	NumCacheNodes int64
	EngineVersion string

	Client elasticacheiface.ElastiCacheAPI
}

type elasticacheRedisConfig struct {
	SubnetGroup struct {
		Name		string
		SubnetIds 	[]string
	}

	Cluster struct {
		Name 			string
		NodeType 		string
		NodePort 		*int64
		NumCacheNodes	int64
		EngineVersion 	string
		SecurityGroupIds []string
	}
}

func (ec *ElasticacheRedisDeploymentStrategy) config() (elasticacheRedisConfig, error) {
	conf := &elasticacheRedisConfig{}

	conf.Cluster.Name = ElasticacheClusterName
	conf.Cluster.NodeType = ec.NodeType
	conf.Cluster.NumCacheNodes = ec.NumCacheNodes
	
	if ec.NodePort == nil {
		defaultPort := int64(6379)
		conf.Cluster.NodePort = &defaultPort
	} else {
		conf.Cluster.NodePort = ec.NodePort
	}
	conf.Cluster.EngineVersion = ec.EngineVersion

	conf.SubnetGroup.Name = ElasticacheClusterSubnetGroupName

	var subnetIds string
	if subnetIds = os.Getenv("VPC_SUBNET_IDS"); subnetIds != "" {
		if strings.ContainsRune(subnetIds,';'){
			conf.SubnetGroup.SubnetIds = strings.Split(subnetIds,";")
		} else {
			conf.SubnetGroup.SubnetIds = []string{subnetIds}
		}	
	} else if subnetIds = viper.GetString("efsVPCSubnetIds"); subnetIds != "" {
		if strings.ContainsRune(subnetIds,';'){
			conf.SubnetGroup.SubnetIds = strings.Split(subnetIds,";")
		} else {
			conf.SubnetGroup.SubnetIds = []string{subnetIds}
		}
	} else if subnetIds == "" {
		log.Error("could not determine vpc subnet ids")
		panic(fmt.Errorf("missing client conif and VPC_SUBNET_IDS not set in enviroment"))
	}

	var securityGroupIds string
	if securityGroupIds = os.Getenv("VPC_SECURITYGROUP_IDS"); securityGroupIds != "" {
		if strings.ContainsRune(securityGroupIds,';'){
			conf.Cluster.SecurityGroupIds = strings.Split(securityGroupIds,";")
		} else {
			conf.Cluster.SecurityGroupIds = []string{securityGroupIds}
		}	
	} else if securityGroupIds = viper.GetString("efsVPCSecurityGroupIds"); securityGroupIds != "" {
		if strings.ContainsRune(securityGroupIds,';'){
			conf.Cluster.SecurityGroupIds = strings.Split(securityGroupIds,";")
		} else {
			conf.Cluster.SecurityGroupIds = []string{securityGroupIds}
		}
	} else if securityGroupIds == "" {
		log.Error("could not determine vpc securitygroup ids")
		panic(fmt.Errorf("missing client conif and VPC_SECURITYGROUP_IDS not set in enviroment"))	}


	return *conf,nil
}

func (ec *ElasticacheRedisDeploymentStrategy) Deploy() (*ClientConfig, error) {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess := session.Must(session.NewSession())
	ec.Client = elasticache.New(sess)
	log.Infof("Elasticache client initialised")

	conf,err := ec.config()
	if err != nil{
		return nil,fmt.Errorf("failed to generate deployment config, %+v",err)
	}


	subnetgroup, err := ec.createSubnetGroup(&conf)
	if err != nil{
		return nil,fmt.Errorf("failed to create subnet group, %+v",err)
	}

	replicationGroup, err := ec.createReplicationGroup(&conf, subnetgroup.CacheSubnetGroupName)
	if err != nil {
		return nil,fmt.Errorf("failed to create replication group cluster, %+v",err)
	}

	redis_conf := ClientConfig{
		Addrs:          nil,
		LambdaSubnetIds: nil,
		LambdaSecurityGroupIds: nil,
	}

	if len(replicationGroup.NodeGroups) != 0 {
		primaryNode := replicationGroup.NodeGroups[0]
		redis_conf.Addrs = []string{
			fmt.Sprintf("%s:%d", *primaryNode.PrimaryEndpoint.Address, *primaryNode.PrimaryEndpoint.Port),
			//fmt.Sprintf("%s:%d", *replicationGroup.ConfigurationEndpoint.Address, *replicationGroup.ConfigurationEndpoint.Port),
		}
	}

	redis_conf.LambdaSubnetIds = conf.SubnetGroup.SubnetIds
	redis_conf.LambdaSecurityGroupIds = conf.Cluster.SecurityGroupIds
	
	return &redis_conf, nil
}

func (ec *ElasticacheRedisDeploymentStrategy) Undeploy() error {
	conf, err := ec.config()
	if err != nil{
		return fmt.Errorf("failed to generate undeployment config, %+v",err)
	}
	inputDeleteReplicationGroup := &elasticache.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String(conf.Cluster.Name),
	}
	toBeDeleted, err := ec.Client.DeleteReplicationGroup(inputDeleteReplicationGroup)
	if err != nil {
		return err
	}
	deletedReplicationGroup := toBeDeleted.ReplicationGroup
	log.Infof("Waiting for cluster to be deleted")

	for deletedReplicationGroup != nil {
		description, err := ec.Client.DescribeReplicationGroups(&elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String(conf.Cluster.Name),
		})
		if err != nil {
			aerr, _ := err.(awserr.Error);
			if aerr.Code() == elasticache.ErrCodeReplicationGroupNotFoundFault {
				inputDeleteCacheSubnetGroup := &elasticache.DeleteCacheSubnetGroupInput{
					CacheSubnetGroupName: aws.String(conf.SubnetGroup.Name),
				}
				_, err = ec.Client.DeleteCacheSubnetGroup(inputDeleteCacheSubnetGroup)	
				if err != nil {
					log.Infof("Failed deleting subnet group. Try deleting in from the AWS console.")
				}	
				log.Infof("Subnet group %s deleted", conf.SubnetGroup.Name)
				break
			}
		}
		deletedReplicationGroup = description.ReplicationGroups[0]
	}

	return nil
}

func (ec *ElasticacheRedisDeploymentStrategy) createSubnetGroup(config *elasticacheRedisConfig) (*elasticache.CacheSubnetGroup, error) {
	inputCreateSubnetGroup := &elasticache.CreateCacheSubnetGroupInput{
		CacheSubnetGroupDescription: aws.String("cluster subnet group"),
		CacheSubnetGroupName:        aws.String(config.SubnetGroup.Name),
		SubnetIds: aws.StringSlice(config.SubnetGroup.SubnetIds),
	}

	subnetgroup, err := ec.Client.CreateCacheSubnetGroup(inputCreateSubnetGroup)
	if err !=nil {
		aerr, _ := err.(awserr.Error);
		if aerr.Code() == elasticache.ErrCodeCacheSubnetGroupAlreadyExistsFault {
			log.Infof("Subnetgroup already exists")
			existingSubnetGroup, err := ec.Client.DescribeCacheSubnetGroups(&elasticache.DescribeCacheSubnetGroupsInput{
				CacheSubnetGroupName: aws.String(config.SubnetGroup.Name),
			})
			if err != nil {
				return nil,err
			}
			return existingSubnetGroup.CacheSubnetGroups[0],nil
		}
		return nil,err
	}
	
	return subnetgroup.CacheSubnetGroup,nil
}

func (ec *ElasticacheRedisDeploymentStrategy) createReplicationGroup(config *elasticacheRedisConfig, subnetgroupName *string) (*elasticache.ReplicationGroup, error){
	inputCreateReplicationGroup := &elasticache.CreateReplicationGroupInput{
		AutomaticFailoverEnabled:    	aws.Bool(true),
		CacheNodeType:             		aws.String(config.Cluster.NodeType),
		CacheSubnetGroupName:      		aws.String(*subnetgroupName), 
		CacheParameterGroupName:		aws.String("default.redis6.x"), // create a Redis default.redis6.x.cluster.on (cluster mode enabled) replication group
		SecurityGroupIds:				aws.StringSlice(config.Cluster.SecurityGroupIds),
		Engine:                    		aws.String("redis"),
		EngineVersion:             		aws.String(config.Cluster.EngineVersion),
		Port:                      		aws.Int64(*config.Cluster.NodePort),
		NumCacheClusters:            	aws.Int64(config.Cluster.NumCacheNodes), // # replication nodes (max. 5) => ReplicasPerNodeGroup 
		//ReplicasPerNodeGroup:			aws.Int64(config.Cluster.NumCacheNodes),
		NumNodeGroups:					aws.Int64(1), // for cluster mode enabled => 1 shard
		ReplicationGroupDescription:	aws.String("A Corral-Redis replication group."),
		ReplicationGroupId:          	aws.String(config.Cluster.Name),
	}

	result, err := ec.Client.CreateReplicationGroup(inputCreateReplicationGroup)
	if err != nil {
		aerr, _ := err.(awserr.Error);
		if aerr.Code() == elasticache.ErrCodeReplicationGroupAlreadyExistsFault   {
			log.Infof("Elasticache cluster already exists")
			existingReplicationGroup, err := ec.Client.DescribeReplicationGroups(&elasticache.DescribeReplicationGroupsInput{
				ReplicationGroupId: aws.String(config.Cluster.Name),
			})
			if err != nil {
				return nil,err
			}
			if needsUpdate(*existingReplicationGroup.ReplicationGroups[0], *inputCreateReplicationGroup) {
				log.Infof("Needs update")
				updateReplicationGroupInput := &elasticache.ModifyReplicationGroupInput  {
					ApplyImmediately: aws.Bool(true),
					ReplicationGroupId: aws.String(config.Cluster.Name),
					CacheNodeType: aws.String(config.Cluster.NodeType),
				}
				modifiedReplicationGroup, err := ec.Client.ModifyReplicationGroup(updateReplicationGroupInput)
				if err != nil {
					return nil,err
				}
				result.ReplicationGroup = modifiedReplicationGroup.ReplicationGroup
			} else {
				log.Infof("No update needed")
				result.ReplicationGroup = existingReplicationGroup.ReplicationGroups[0]
				return result.ReplicationGroup, nil
			}
		} else {
			return nil,err
		}
	}
	log.Infof("Replication group created; waiting for replication group to be avaiable")

	for *result.ReplicationGroup.Status != "available" {
		description, err := ec.Client.DescribeReplicationGroups(&elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId : aws.String(*result.ReplicationGroup.ReplicationGroupId),
		})
		result.ReplicationGroup = description.ReplicationGroups[0]
		if err !=nil {
			fmt.Println(err)
	
		}
	}
	return result.ReplicationGroup, nil
}

func needsUpdate(cluster elasticache.ReplicationGroup, config elasticache.CreateReplicationGroupInput) bool {
	// scale up or down node type
	log.Infof("Type: %s", *cluster.CacheNodeType)
	log.Infof("Type: %s", *config.CacheNodeType)

	// scale up or down node type
	//log.Infof("#Nodes: %d", *cluster.NumCacheNodes)
	//log.Infof("#Nodes: %d", *config.NumCacheNodes)

	// NOT recomanded to change cluster's engine version => only upgrade possible
	//log.Infof("Engine %s", *cluster.EngineVersion)
	//log.Infof("Engine %s", *config.EngineVersion)

	//log.Infof("Name %s", *cluster.CacheSubnetGroupName)
	//log.Infof("Name %s", *config.CacheSubnetGroupName)

	//return *cluster.CacheNodeType != *config.CacheNodeType || *cluster.CacheSubnetGroupName != *config.CacheSubnetGroupName //|| *cluster.EngineVersion != *config.EngineVersion

	return *cluster.CacheNodeType != *config.CacheNodeType
}
