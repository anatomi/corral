package corcache

import (
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)


// FileSystemType is an identifier for supported FileSystems
type CacheSystemType int

// Identifiers for supported FileSystemTypes
const (
	NoCache CacheSystemType = iota
	Local
	Redis
	Olric
	EFS
)

type CacheConfigIncector interface {
	CacheSystem() CacheSystem
}

//CacheSystem represent a ephemeral file system used for intermidiate state between map/reduce phases
type CacheSystem interface {
	corfs.FileSystem

	Deploy() error
	Undeploy() error

	Flush(system corfs.FileSystem) error
	Clear() error

	//FunctionInjector can be used by function deployment code to modify function deploymentes to use the underling cache system, warning needs to be implmented for each platfrom induvidually
	FunctionInjector() CacheConfigIncector
}

// NewCacheSystem intializes a CacheSystem of the given type
func NewCacheSystem(fsType CacheSystemType) (CacheSystem,error) {
	var cs CacheSystem
	var err error
	switch fsType {
		//this implies that
		case NoCache:
			log.Info("No CacheSystem availible, using FileSystme as fallback")
			return nil,nil
		case Local:
			//TODO: make this configururable
			cs = NewLocalInMemoryProvider(viper.GetUint64("cacheSize"))
	case Redis:
		cs,err = NewRedisBackedCache(DeploymentType(viper.GetInt("redisDeploymentType")))
		if err != nil {
			log.Debug("failed to init redis cache, %+v",err)
			return nil,err
		}

	default:
		return nil, fmt.Errorf("unknown cache type or not yet implemented %d",fsType)
	}

	return cs,nil
}

// CacheSystemTypes retunrs a type for a given CacheSystem or the NoCache type.
func CacheSystemTypes(fs CacheSystem) CacheSystemType {
	if fs == nil{
		return NoCache
	}

	if _,ok := fs.(*LocalCache);ok {
		return Local
	}
	return NoCache
}
