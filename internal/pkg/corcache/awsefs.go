package corcache

import (
	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/spf13/viper"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/aws/aws-sdk-go/service/efs/efsiface"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"io"
	log "github.com/sirupsen/logrus"
	"os"
	"fmt"
	"strings"
	"path/filepath"
)

type AWSEfsConfig struct {
	FilesystemName				string
	AccessPointName 			string
	AccessPointPath 			string
	AccessPointArn				string
	LambdaEfsPath				string
	VpcSubnetIds    			[]string
	VpcSecurityGroupIds			[]string
}

type AWSEFSCache struct {
	Client efsiface.EFSAPI
	FileSystem *efs.FileSystemDescription
	AccessPoint *efs.AccessPointDescription
	MountTargets []*efs.MountTargetDescription 
	Config *AWSEfsConfig
}

func walkDir(dir string) []corfs.FileInfo {
	files := make([]corfs.FileInfo, 0)
	filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Error(err)
			return err
		}
		if f.IsDir() {
			return nil
		}
		files = append(files, corfs.FileInfo{
			Name: path,
			Size: f.Size(),
		})
		return nil
	})

	return files
}

func NewEFSCache() (*AWSEFSCache,error) {
	return &AWSEFSCache{
	},nil
}

// initalize EFS client
func (A *AWSEFSCache) Init() error {
	if A.Client != nil{
		log.Debug("EFS client was already initialized")
		return nil
	}

	if A.Config == nil {
		conf := AWSEfsConfig{}

		fail := func(key string) error {
			return fmt.Errorf("missing client config and %s not set in enviroment",key)
		}

		filesystemName := os.Getenv("FILESYSTEM_NAME")
		if filesystemName != "" {
			conf.FilesystemName = filesystemName
		} else {
			return fail("FILESYSTEM_NAME")
		}

		accessPointName := os.Getenv("ACCESS_POINT_NAME")
		if accessPointName != "" {
			conf.AccessPointName = accessPointName
		} else {
			return fail("ACCESS_POINT_NAME")
		}

		accessPointPath := os.Getenv("ACCESS_POINT_PATH")
		if accessPointPath != "" {
			conf.AccessPointPath = accessPointPath
		} else {
			return fail("ACCESS_POINT_PATH")
		}

		filesystemPath := os.Getenv("FILESYSTEM_PATH")
		if filesystemPath != "" {
			conf.LambdaEfsPath = filesystemPath
		} else {
			return fail("FILESYSTEM_PATH")
		}

		A.Config = &conf
	}

	return A.NewEfsClient()
}

// initialize EFS cache configuration; create new Filesyste, MountTargets and Accesspoint for Lambda
func (A *AWSEFSCache) Deploy() error {
	log.Infof("Init EFS Cache")

	conf := AWSEfsConfig{}

	fail := func(key string) error {
		return fmt.Errorf("missing client config and %s not set in config.go",key)
	}

	var subnetIds string
	if subnetIds = viper.GetString("VPCSubnetIds"); subnetIds != "" {
		if strings.ContainsRune(subnetIds,';'){
			conf.VpcSubnetIds = strings.Split(subnetIds,";")
		} else {
			conf.VpcSubnetIds = []string{subnetIds}
		}
	} else if subnetIds == "" {
		return fail("VPCSubnetIds")
	}

	var securityGroupIds string
	if securityGroupIds = viper.GetString("VPCSecurityGroupIds"); securityGroupIds != "" {
		if strings.ContainsRune(securityGroupIds,';'){
			conf.VpcSecurityGroupIds = strings.Split(securityGroupIds,";")
		} else {
			conf.VpcSecurityGroupIds = []string{securityGroupIds}
		}
	} else if securityGroupIds == "" {
		return fail("VPCSecurityGroupIds")
	}

	conf.FilesystemName = viper.GetString("efsFilesystemName")
	conf.AccessPointName = viper.GetString("efsAccessPointName")
	conf.AccessPointPath = viper.GetString("efsAccessPointPath")
	conf.LambdaEfsPath = viper.GetString("lambdaEfsPath")

	A.Config = &conf

	A.NewEfsClient()

	err := A.InitEfsFilesystem(A.Config)
	if err != nil {
		return fmt.Errorf("Filesystem could not be created: %s", err)
	}
	err = A.InitMountTargets(A.Config)
	if err != nil {
		return fmt.Errorf("MountTargets could not be created: %s", err)
	}
	err = A.InitAccessPoint(A.Config)
	if err != nil {
		return fmt.Errorf("AccessPoint could not be created: %s", err)
	}
	return nil
}

func (A *AWSEFSCache) Undeploy() error {
	// EFS efsiface does not have any methods to close EFS clients or delete filesystem, mount targets or access points
	// You can't delete a file system that is in use. That is, if the file system has any mount targets, 
	// you must first delete them. For more information, see DescribeMountTargets and DeleteMountTarget.
	// Deleting mount targets 
	mountTargetsCount := len(A.MountTargets)
	for _, mountTarget := range A.MountTargets {
		deleteMountTargetInput := &efs.DeleteMountTargetInput{
			MountTargetId: aws.String(*mountTarget.MountTargetId),
		}
		
		_, err := A.Client.DeleteMountTarget(deleteMountTargetInput)
		if err != nil {
			return err
		}
	}
	log.Infof("Waiting for mount targets to be deleted")        


	interval := setInterval(func() {
		describeInput := &efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(*A.FileSystem.FileSystemId),
		}
		result, err := A.Client.DescribeMountTargets(describeInput)
		if err != nil {
			panic(err)
		}

		if len(result.MountTargets) == 0 {
			mountTargetsCount = len(result.MountTargets)
		}
	}, 10000, false)

	for {
		if mountTargetsCount == 0 {
			log.Infof("Mount targets deleted")
			deleteFileSystemInput := &efs.DeleteFileSystemInput{
				FileSystemId: aws.String(*A.FileSystem.FileSystemId),
			}

			log.Infof("Deleting file system")
			_, err := A.Client.DeleteFileSystem(deleteFileSystemInput)
			if err != nil {
				panic(err)
			}
			interval <- true
			return nil
		}
	}
	
	return nil
}

func (A *AWSEFSCache) ListFiles(pathGlob string) ([]corfs.FileInfo, error) {
	globbedFiles, err := filepath.Glob(pathGlob)
	if err != nil {
		return nil, err
	}

	files := make([]corfs.FileInfo, 0)
	for _, fileName := range globbedFiles {
		fInfo, err := os.Stat(fileName)
		if err != nil {
			log.Error(err)
			continue
		}
		if !fInfo.IsDir() {
			files = append(files, corfs.FileInfo{
				Name: fileName,
				Size: fInfo.Size(),
			})
		} else {
			files = append(files, walkDir(fileName)...)
		}
	}

	return files, err
}

func (A *AWSEFSCache) Stat(path string) (corfs.FileInfo, error) {
	fInfo, err := os.Stat(path)
	if err != nil {
		return corfs.FileInfo{}, err
	}
	return corfs.FileInfo{
		Name: path,
		Size: fInfo.Size(),
	}, nil
}

func (A *AWSEFSCache) OpenReader(path string, startAt int64) (io.ReadCloser, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(startAt, io.SeekStart)
	return file, err
}

func (A *AWSEFSCache) OpenWriter(path string) (io.WriteCloser, error) {
	dir := filepath.Dir(path)

	// Create writer directory if necessary
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0777)
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
}

func (A *AWSEFSCache) Delete(path string) error {
	return os.Remove(path)
}

func (A *AWSEFSCache) Join(elem ...string) string {
	return strings.Join(elem,"/")
}

func (A *AWSEFSCache) Split(path string) []string {
	return strings.Split(path,"/")
}

func (A *AWSEFSCache) Flush(fs corfs.FileSystem, outputPath string) error {
	files, err := A.ListFiles(A.Config.LambdaEfsPath)
	if err != nil {
		return err
	}
	bytesMoved := int64(0)
	for _,file := range files {
		path := file.Name
		pathComponents := A.Split(file.Name)
		destPath := fs.Join(outputPath, pathComponents[len(pathComponents)-1])
		writer,err := fs.OpenWriter(destPath)
		if err != nil {
			return err
		}
		defer writer.Close()

		reader,err := A.OpenReader(path,0)
		if err != nil{
			return err
		}
		defer reader.Close()
		moved,err := io.Copy(writer,reader)
		if err != nil {
			return err
		}
		bytesMoved+=moved

	}
	log.Infof("Flushed %d bytes to backend",bytesMoved)

	return nil
}

func (A *AWSEFSCache) Clear() error {
	files, err := A.ListFiles(A.Join(A.Config.LambdaEfsPath, "*"))
    if err != nil {
        return err
    }

    for _, file := range files {
        err = os.RemoveAll(file.Name)
        if err != nil {
            return err
        }
    }
    return nil
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

// add VPC configuration for Lambda
func (a *AWSEFSCacheConfigInjector) ConfigureLambda(functionConfig *lambda.CreateFunctionInput) error {	
	functionConfig.Environment.Variables["FILESYSTEM_NAME"] = &a.system.Config.FilesystemName
	functionConfig.Environment.Variables["ACCESS_POINT_NAME"] = &a.system.Config.AccessPointName
	functionConfig.Environment.Variables["ACCESS_POINT_PATH"] = &a.system.Config.AccessPointPath
	functionConfig.Environment.Variables["FILESYSTEM_PATH"] = &a.system.Config.LambdaEfsPath

	filesystemConfig := &lambda.FileSystemConfig{
		Arn: a.system.AccessPoint.AccessPointArn,
		LocalMountPath: aws.String(a.system.Config.LambdaEfsPath),
	}
	functionConfig.SetFileSystemConfigs([]*lambda.FileSystemConfig{filesystemConfig})

	vpcConfig := &lambda.VpcConfig{
		SecurityGroupIds: aws.StringSlice(a.system.Config.VpcSecurityGroupIds),
		SubnetIds: aws.StringSlice(a.system.Config.VpcSubnetIds),
	}
	functionConfig.SetVpcConfig(vpcConfig)	
	return nil
}

func (A *AWSEFSCache) NewEfsClient() error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess := session.Must(session.NewSession())
	A.Client = efs.New(sess)
	log.Info("EFS client initialised")

	return nil
}


func (A *AWSEFSCache) InitEfsFilesystem(config *AWSEfsConfig) error {
	fileSystems, _ := A.Client.DescribeFileSystems(&efs.DescribeFileSystemsInput{})
	for _, fs := range fileSystems.FileSystems {
		if *fs.Name == config.FilesystemName {
			log.Infof("EFS filesystem exists")
			A.FileSystem = fs
			break;
		}
	}

	if A.FileSystem == nil {
		log.Infof("Creating new EFS filesystem")
		createInput := &efs.CreateFileSystemInput{
			Backup:          aws.Bool(true),
			CreationToken:   aws.String(randomToken(10)),
			Encrypted:       aws.Bool(true),
			PerformanceMode: aws.String("generalPurpose"),
			ThroughputMode: aws.String("bursting"),
			Tags: []*efs.Tag{
				{
					Key:   aws.String("Name"),
					Value: aws.String(config.FilesystemName),
				},
			},
		}

		createResult, err := A.Client.CreateFileSystem(createInput)
		if err != nil {
			return err
		}
		A.FileSystem = createResult

		filesystemAvailable := false
		log.Infof("Waiting for file system to be avaialble")        
		interval := setInterval(func() {
			describeInput := &efs.DescribeFileSystemsInput{
				FileSystemId: A.FileSystem.FileSystemId,
			}
			result, err := A.Client.DescribeFileSystems(describeInput)
			if err != nil {
				panic(err)
			}
			
			A.FileSystem = result.FileSystems[0];
			if *A.FileSystem.LifeCycleState == "available" {
				filesystemAvailable = true;
			}
		}, 2000, false)
			
		for {
			if filesystemAvailable {
				log.Infof("EFS Filesystem is avaiable")
				interval <- true
				return nil
			}
		}
	}
	return nil
}

func (A *AWSEFSCache) InitMountTargets(config *AWSEfsConfig) error {
	for _, subnetId := range config.VpcSubnetIds {
		mountInput := &efs.CreateMountTargetInput{
			FileSystemId: A.FileSystem.FileSystemId,
			SubnetId: aws.String(subnetId),
		}
		mountResult, err := A.Client.CreateMountTarget(mountInput)
		if err != nil {
			aerr, _ := err.(awserr.Error);
			if aerr.Code() == efs.ErrCodeMountTargetConflict {
				log.Infof("EFS Mount targets already exists")
				mountTargets, err := A.Client.DescribeMountTargets(&efs.DescribeMountTargetsInput{
					FileSystemId: A.FileSystem.FileSystemId,
				})
				if err != nil {
					return err
				}
				A.MountTargets = mountTargets.MountTargets
				return nil

			}
			return err
		}
		A.MountTargets = append(A.MountTargets, mountResult)
	}
	log.Infof("EFS Mount targets created")

	mountTargetsAvailable := false
	log.Infof("Waiting for mount targets to be avaialble")        
	interval := setInterval(func() {
		describeInput := &efs.DescribeMountTargetsInput{
			FileSystemId: A.FileSystem.FileSystemId,
		}
		result, err := A.Client.DescribeMountTargets(describeInput)
		if err != nil {
			panic(err)
		}
		countAvailableMountTargets := 0
		for _, mountTarget := range result.MountTargets {
			if *mountTarget.LifeCycleState == "available" {
				countAvailableMountTargets = countAvailableMountTargets + 1
			}
		}

		if countAvailableMountTargets == len(result.MountTargets) {
			mountTargetsAvailable = true
			A.MountTargets = result.MountTargets
		}
	}, 10000, false)
        
	for {
		if mountTargetsAvailable {
			log.Infof("Mount targets are avaialble")
			interval <- true
			return nil
		}
	}
	return nil	
}

func (A *AWSEFSCache) InitAccessPoint(config *AWSEfsConfig) error {
	accessPointInput := &efs.CreateAccessPointInput{
		ClientToken: aws.String(randomToken(10)),
		FileSystemId: A.FileSystem.FileSystemId,
		PosixUser : &efs.PosixUser{
			Gid: aws.Int64(1001),
			Uid: aws.Int64(1001),
		},
		RootDirectory : &efs.RootDirectory{ 
			CreationInfo: &efs.CreationInfo{
				OwnerGid: aws.Int64(1001),
				OwnerUid: aws.Int64(1001),
				Permissions: aws.String("0777"),
			},
			Path: aws.String(config.AccessPointPath),
		},
		Tags: []*efs.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(config.AccessPointName),
			},
		},
	}
	
	accesPointResult, err := A.Client.CreateAccessPoint(accessPointInput) // type: AccessPointOutput
	if err != nil {
		aerr, _ := err.(awserr.Error);
		if aerr.Code() == efs.ErrCodeAccessPointAlreadyExists {
			log.Infof("EFS Access Point already exists")

			accesPoints, err := A.Client.DescribeAccessPoints(&efs.DescribeAccessPointsInput{
				FileSystemId: A.FileSystem.FileSystemId,
			})
			if err != nil {
				return err
			}
			A.AccessPoint = accesPoints.AccessPoints[0] // type: AccessPointDescription
			return nil

		}
		return err
	}
	// converting AccessPointOutput to AccessPointDescription (issue with aws golang sdk !!!)
	accessPointDescription := &efs.AccessPointDescription{
		AccessPointArn: accesPointResult.AccessPointArn, 
		AccessPointId: accesPointResult.AccessPointId, 
		ClientToken: accesPointResult.ClientToken, 
		FileSystemId: accesPointResult.FileSystemId, 
		LifeCycleState: accesPointResult.LifeCycleState, 
		Name: accesPointResult.Name, 
		OwnerId: accesPointResult.OwnerId, 
		PosixUser: accesPointResult.PosixUser, 
		RootDirectory: accesPointResult.RootDirectory,
		Tags: accesPointResult.Tags,  
	}
	A.AccessPoint = accessPointDescription
	log.Infof("EFS Access Point created")
	return nil
}