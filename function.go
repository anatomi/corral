package corral

import (
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corcache"
	"runtime/debug"
	"time"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
)

//takes a driver and a
func handle(driver *Driver, hostID func() string, requestID func() string) func(task task) (taskResult, error) {
	return func(task task) (taskResult, error) {
		estart := time.Now()
		// Precaution to avoid running out of memory for reused Lambdas
		debug.FreeOSMemory()
		result := taskResult{
			HId:    hostID(),
			CId:    driver.runtimeID,
			RId:    requestID(),
			JId:    fmt.Sprintf("%d_%d_%d", task.JobNumber, task.Phase, task.BinID),
			CStart: driver.Start.UnixNano(),
			EStart: estart.UnixNano(),
		}
		// Setup current job
		//we can assume that this dose not change all the time
		var err error
		fs, err := corfs.InitFilesystem(task.FileSystemType)
		if err != nil {
			result.EEnd = time.Now().UnixNano()
			return result, err
		}
		log.Infof("%d - %+v", task.FileSystemType, task)
		currentJob := driver.jobs[task.JobNumber]

		cache, err := corcache.NewCacheSystem(task.CacheSystemType)
		if err != nil {
			result.EEnd = time.Now().UnixNano()
			return result, err
		}

		driver.currentJob = task.JobNumber
		currentJob.fileSystem = fs
		currentJob.cacheSystem = cache
		currentJob.intermediateBins = task.IntermediateBins
		currentJob.outputPath = task.WorkingLocation
		currentJob.config.Cleanup = task.Cleanup

		// Need to reset job counters in case this is a reused lambda
		currentJob.bytesRead = 0
		currentJob.bytesWritten = 0

		switch task.Phase {
		case MapPhase:
			err = currentJob.runMapper(task.BinID, task.Splits)
		case ReducePhase:
			err = currentJob.runReducer(task.BinID)
		default:
			err = fmt.Errorf("Unknown phase: %d", task.Phase)
		}

		result.BytesRead = int(currentJob.bytesRead)
		result.BytesWritten = int(currentJob.bytesWritten)
		result.EEnd = time.Now().UnixNano()

		return result, err
	}
}
