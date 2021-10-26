package corcache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/apache/openwhisk-client-go/whisk"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"strconv"
	"strings"
)

type DeploymentType int

const (
	LocalDeployment DeploymentType = iota
	KubernetesDeployment
	AWS
)

func NewDeploymentStrategy(deploymentType DeploymentType) (DeploymentStrategy,error) {
	switch deploymentType {
		case LocalDeployment:
			log.Debug("using local-docker redis deployment")
			return &LocalRedisDeploymentStrategy{},nil
		case KubernetesDeployment:
			log.Debug("using kubernatis based deployment")
			krds := &KubernetesRedisDeploymentStrategy{
				Namespace:      viper.GetString("kubernetesNamespace"),
				StorageClass:   viper.GetString("kubernetesStorageClass"),
			}

			if viper.IsSet("redisPort") {
				foo := viper.GetInt("redisPort")
				krds.NodePort = &foo
			}

			return krds,nil
		case AWS:
			return nil,fmt.Errorf("not yet implemented")
		default:
			return nil,fmt.Errorf("unexpected deploymentType: %+v",deploymentType)
	}
}

type ClientConfig struct {
	Addrs          []string
	DB             int
	User           string
	password       string
	RouteByLatency bool
	RouteRandomly  bool
}

func (rc *ClientConfig) asOptions() *redis.UniversalOptions {
	return &redis.UniversalOptions{
		Addrs: rc.Addrs,
		DB: rc.DB,
		Username: rc.User,
		Password: rc.password,
		RouteByLatency: rc.RouteByLatency,
		RouteRandomly:  rc.RouteRandomly,


	}
}

type DeploymentStrategy interface {
	Deploy() (*ClientConfig,error)
	Undeploy() error
}

type RedisBackedCache struct {
	DeploymentStragey DeploymentStrategy
	Client            redis.UniversalClient
	Config            *ClientConfig
}

func NewRedisBackedCache(deploymentType DeploymentType) (*RedisBackedCache,error) {
	ds,err := NewDeploymentStrategy(deploymentType)
	if err != nil {
		log.Debug("failed to create deployment strategy for redis backend")
		return nil,err
	}
	log.Infof("using %+v redis deployment strategy",deploymentType)

	return &RedisBackedCache{
		DeploymentStragey: ds,
	},nil

}


func (r *RedisBackedCache) Init() error {
	if r.Client != nil{
		log.Debug("init redis cache that was already initialized")
		return nil
	}


	//we have no config, lets try to make one from the enviroment or fail
	if r.Config == nil {
		conf := ClientConfig{}

		fail := func(key string) error {
			return fmt.Errorf("missing client conif and %s not set in enviroment",key)
		}

		addrs := os.Getenv("REDIS_ADDRS")
		if addrs != "" {
			if strings.ContainsRune(addrs,';'){
				conf.Addrs = strings.Split(addrs,";")
			} else {
				conf.Addrs = []string{addrs}
			}
		} else {
			return fail("REDIS_ADDRS")
		}

		db := os.Getenv("REDIS_DB")
		if db != ""{
			dbp,err := strconv.ParseInt(db,10,32)
			if err != nil {
				return err
			}
			conf.DB = int(dbp)
		} else {
			return fail("REDIS_DB")
		}

		user := os.Getenv("REDIS_USER")
		if user != "" {
			conf.User = user
		} else {
			return fail("REDIS_USER")
		}

		//TODO: XXXX this is not a good practice, we could compile this in code and fail in local settings, but for now it is what it is...
		secret := os.Getenv("REDIS_SECRET")
		if user != "" {
			conf.password = secret
		} else {
			return fail("REDIS_SECRET")
		}

		var mode int = 0
		if os.Getenv("REDIS_MODE") != ""{
			mode_value,err := strconv.ParseInt(os.Getenv("REDIS_MODE"),10,32)
			if err == nil {
				mode = int(mode_value)
			}
		}

		conf.RouteByLatency = 2 == mode & 2
		conf.RouteRandomly = 1 == mode & 1

		r.Config = &conf
	}

	r.Client = redis.NewUniversalClient(r.Config.asOptions())

	_, err := r.Client.Ping(context.Background()).Result()
	return err
}


func (r *RedisBackedCache) ListFiles(pathGlob string) ([]corfs.FileInfo, error) {
	results := make([]corfs.FileInfo,0)
	scan := r.Client.Scan(context.Background(),0,pathGlob,0)
	itter := scan.Iterator()
	for itter.Next(context.Background()) {
		if itter.Err() != nil {
			return nil,itter.Err()
		}

		name := itter.Val()

		s,err := r.Stat(name)
		if err != nil{
			return nil, err
		}
		results = append(results,s)
	}
	return results,nil
}

func (r *RedisBackedCache) Stat(filePath string) (corfs.FileInfo, error) {
	size,err := r.Client.StrLen(context.Background(),filePath).Result()
	if err != nil{
		return corfs.FileInfo{}, err
	}

	if size <= 0 {
		return corfs.FileInfo{}, fmt.Errorf("file dose not exsist")
	}

	return corfs.FileInfo{
		Name: filePath,
		Size: size,
	},nil
}

//instead of reading everything at once, only read part of the file at once..
type bufferedRedisReader struct {
	*bytes.Buffer
}

func (b *bufferedRedisReader) Close() error {
	b.Buffer.Reset()
	return nil
}

func (r *RedisBackedCache) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	buf,err := r.Client.Get(context.Background(),filePath).Bytes()
	if err != nil {
		return nil,err
	} else {
		reader := &bufferedRedisReader{
			Buffer: bytes.NewBuffer(buf),
		}
		if startAt > 0 {
			_ = reader.Next(int(startAt))
		}
		return reader,nil
	}
}

//Use a buffer but instead of writing all at once read the data in invervals
type bufferedRedisWriter struct {
	*bytes.Buffer
	key string
	client redis.UniversalClient
}

func (b *bufferedRedisWriter) Close() error {
	bytes := b.Bytes()
	msg,err := b.client.Set(context.Background(),b.key,bytes,0).Result()
	log.Debug(msg)
	return err
}

func (r *RedisBackedCache) newRedisWriter(key string,buffer []byte) *bufferedRedisWriter {
	if buffer == nil{
		buffer = []byte{}
	}
	return &bufferedRedisWriter{
		Buffer: bytes.NewBuffer(buffer),
		key:    key,
		client: r.Client,
	}
}

func (r *RedisBackedCache) OpenWriter(filePath string) (io.WriteCloser, error) {

	buf,err := r.Client.Get(context.Background(),filePath).Bytes()
	if err != nil{
		//TODO: is that correct?
		return r.newRedisWriter(filePath,nil),nil
	} else {
		return r.newRedisWriter(filePath,buf),nil
	}
}

func (r *RedisBackedCache) Delete(filePath string) error {
	d,err := r.Client.Del(context.Background(),filePath).Result()
	if err != nil {
		return err
	}
	if d <= 0 {
		return fmt.Errorf("file dose not exist")
	}
	return err
}

func (r *RedisBackedCache) Join(elem ...string) string {
	return strings.Join(elem,"/")
}

func (r *RedisBackedCache) Split(path string) []string {
	return strings.Split(path,"/")
}


func (r *RedisBackedCache) Deploy() error {
	conf,err := r.DeploymentStragey.Deploy()
	if err != nil{
		return err
	}

	r.Config = conf

	return nil

}

func (r *RedisBackedCache) Undeploy() error {
	r.Client.Close()
	return r.DeploymentStragey.Undeploy()
}

func (r *RedisBackedCache) Flush(fs corfs.FileSystem) error {
	scan := r.Client.Scan(context.Background(),0,"*",0)
	itter := scan.Iterator()

	bytesMoved := int64(0)
	for itter.Next(context.Background()) {
		if itter.Err() != nil {
			return itter.Err()
		}

		path := itter.Val()
		destPath := fs.Join(r.Split(path)...)
		writer,err := fs.OpenWriter(destPath)
		if err != nil {
			return err
		}
		defer writer.Close()

		reader,err := r.OpenReader(path,0)
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

func (r *RedisBackedCache) Clear() error {
	scan := r.Client.Scan(context.Background(),0,"*",0)
	itter := scan.Iterator()
	keys := make([]string,0)
	for itter.Next(context.Background()) {
		if itter.Err() != nil {
			return itter.Err()
		}
		keys = append(keys,itter.Val())
	}
	if len(keys) > 0 {
		_, err := r.Client.Del(context.Background(), keys...).Result()
		return err
	}
	return nil
}

func (r *RedisBackedCache) FunctionInjector() CacheConfigIncector {
	return &RedisCacheConfigInjector{system: r}
}

type RedisCacheConfigInjector struct {
	system *RedisBackedCache

}



//WE strongly assume astion.Paramters are injected at runtime...
func (r *RedisCacheConfigInjector) ConfigureWhisk(action *whisk.Action) error {
	if r.system == nil {
		return fmt.Errorf("RedisBackedCache Reference missing")
	}

	if r.system.Config == nil {
		return fmt.Errorf("Cache Config not availible")
	}

	addrs  := strings.Join(r.system.Config.Addrs,";")
	action.Parameters.AddOrReplace(&whisk.KeyValue{
		Key:   "REDIS_ADDRS",
		Value: addrs,
	})

	action.Parameters.AddOrReplace(&whisk.KeyValue{
		Key:   "REDIS_DB",
		Value: r.system.Config.DB,
	})

	action.Parameters.AddOrReplace(&whisk.KeyValue{
		Key:   "REDIS_USER",
		Value: r.system.Config.User,
	})

	action.Parameters.AddOrReplace(&whisk.KeyValue{
		Key:   "REDIS_SECRET",
		Value: &r.system.Config.password,
	})

	return nil
}

func (r *RedisCacheConfigInjector) ConfigureLambda(function *lambda.CreateFunctionInput) error {

	if r.system == nil {
		return fmt.Errorf("RedisBackedCache Reference missing")
	}

	if r.system.Config == nil {
		return fmt.Errorf("Cache Config not availible")
	}

	addrs  := strings.Join(r.system.Config.Addrs,";")
	function.Environment.Variables["REDIS_ADDRS"] = &addrs

	db := fmt.Sprintf("%d",r.system.Config.DB)
	function.Environment.Variables["REDIS_DB"] = &db


	function.Environment.Variables["REDIS_USER"] = &r.system.Config.User
	function.Environment.Variables["REDIS_SECRET"] = &r.system.Config.password

	mode := 0
	if r.system.Config.RouteRandomly {
		mode = mode | 1
	}
	if r.system.Config.RouteByLatency {
		mode = mode | 2
	}
	modeFlag := fmt.Sprintf("%d",mode)
	function.Environment.Variables["REDIS_MODE"] = &modeFlag

	return nil
}

func (r *RedisCacheConfigInjector) CacheSystem() CacheSystem {
	return r.system
}
