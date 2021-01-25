package corral

import (
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corfs"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
	"time"
)

//takes a driver and a
func handle(driver *Driver,hostId func () string) func(task task) (taskResult, error) {
	return func(task task) (taskResult, error) {
		estart := time.Now()
		// Precaution to avoid running out of memory for reused Lambdas
		debug.FreeOSMemory()

		// Setup current job
		fs := corfs.InitFilesystem(task.FileSystemType)
		log.Infof("%d - %+v", task.FileSystemType, task)
		currentJob := driver.jobs[task.JobNumber]
		currentJob.fileSystem = fs
		currentJob.intermediateBins = task.IntermediateBins
		currentJob.outputPath = task.WorkingLocation
		currentJob.config.Cleanup = task.Cleanup

		// Need to reset job counters in case this is a reused lambda
		currentJob.bytesRead = 0
		currentJob.bytesWritten = 0

		var err error
		switch task.Phase {
			case MapPhase:
				err = currentJob.runMapper(task.BinID, task.Splits)
			case ReducePhase:
				err = currentJob.runReducer(task.BinID)
			default:
				err = fmt.Errorf("Unknown phase: %d", task.Phase)
		}
		eend := time.Now()
		result := taskResult{
			BytesRead:    int(currentJob.bytesRead),
			BytesWritten: int(currentJob.bytesWritten),
			HId:          hostId(),
			CId:          driver.runtimeID,
			JId:          fmt.Sprintf("%d_%d", task.Phase, task.BinID),
			CStart:       driver.Start.Unix(),
			EStart:       estart.Unix(),
			EEnd:         eend.Unix(),
		}
		return result, err
	}
}
