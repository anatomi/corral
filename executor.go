package corral

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"time"
)

type executor interface {
	RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error
	RunReducer(job *Job, jobNumber int, binID uint) error

	HintSplits(splits uint) error
}

type smileExecuter interface {
	executor
	BatchRunMapper(job *Job, jobNumber int, inputSplits [][]inputSplit) error
	BatchRunReducer(job *Job, jobNumber int, bins []uint) error
	//TODO: implement Join/Split/Shuffle?
}

type localExecutor struct {
	Start time.Time
}

func (l *localExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	estart := time.Now()
	// Precaution to avoid running out of memory for reused Lambdas
	debug.FreeOSMemory()

	err := job.runMapper(binID, inputSplits)

	eend := time.Now()
	result := taskResult{
		BytesRead:    int(job.bytesRead),
		BytesWritten: int(job.bytesWritten),
		HId:          "local",
		CId:          "local",
		JId:          fmt.Sprintf("%d_%d_%d", jobNumber, 0, binID),
		RId:          strconv.Itoa(jobNumber),
		CStart:       l.Start.Unix(),
		EStart:       estart.Unix(),
		EEnd:         eend.Unix(),
	}

	job.collectActivation(result)
	return err
}

func (l *localExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	estart := time.Now()
	// Precaution to avoid running out of memory for reused Lambdas
	debug.FreeOSMemory()

	err := job.runReducer(binID)

	eend := time.Now()
	result := taskResult{
		BytesRead:    int(job.bytesRead),
		BytesWritten: int(job.bytesWritten),
		HId:          "local",
		CId:          "local",
		JId:          fmt.Sprintf("%d_%d_%d", jobNumber, 1, binID),
		RId:          strconv.Itoa(jobNumber),
		CStart:       l.Start.Unix(),
		EStart:       estart.Unix(),
		EEnd:         eend.Unix(),
	}

	job.collectActivation(result)
	return err
}

func (l *localExecutor) HintSplits(splits uint) error {
	return nil
}
