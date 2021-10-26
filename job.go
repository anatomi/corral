package corral

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corcache"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anatomi/corral/internal/pkg/corfs"
	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// Job is the logical container for a MapReduce job
type Job struct {
	Map           Mapper
	Reduce        Reducer
	PartitionFunc PartitionFunc
	PauseFunc     PauseFunc
	StopFunc      StopFunc
	HintFunc      HintFunc

	fileSystem  corfs.FileSystem
	cacheSystem corcache.CacheSystem

	config           *config
	intermediateBins uint
	outputPath       string

	bytesRead    int64
	bytesWritten int64

	activationLog chan taskResult
	wg            sync.WaitGroup
}

func (j *Job) collectActivation(result taskResult) {
	if j.activationLog != nil {
		j.activationLog <- result
	}
}

// Logic for running a single map task
func (j *Job) runMapper(mapperID uint, splits []inputSplit) error {
	//check if we can use a cacheFS instead
	var fs corfs.FileSystem = j.fileSystem
	if j.cacheSystem != nil {
		fs = j.cacheSystem
	}

	emitter := newMapperEmitter(j.intermediateBins, mapperID, j.outputPath, fs)
	if j.PartitionFunc != nil {
		emitter.partitionFunc = j.PartitionFunc
	}

	for _, split := range splits {
		err := j.runMapperSplit(split, &emitter)
		if err != nil {
			return err
		}
	}

	atomic.AddInt64(&j.bytesWritten, emitter.bytesWritten())
	return emitter.close()
}

func splitInputRecord(record string) *keyValue {
	//XXX: in case of map this will just cost a lot of unnassary compute...
	fields := strings.Split(record, "\t")
	if len(fields) == 2 {
		return &keyValue{
			Key:   fields[0],
			Value: fields[1],
		}
	}
	return &keyValue{
		Value: record,
	}
}

// runMapperSplit runs the mapper on a single inputSplit
func (j *Job) runMapperSplit(split inputSplit, emitter Emitter) error {
	offset := split.StartOffset
	if split.StartOffset != 0 {
		offset--
	}

	inputSource, err := j.fileSystem.OpenReader(split.Filename, split.StartOffset)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(inputSource)
	var bytesRead int64
	splitter := countingSplitFunc(bufio.ScanLines, &bytesRead)
	scanner.Split(splitter)

	if split.StartOffset != 0 {
		scanner.Scan()
	}

	for scanner.Scan() {
		record := scanner.Text()
		kv := splitInputRecord(record)
		//inject the filename in case we have no other key...
		if kv.Key == "" {
			kv.Key = split.Filename
		}
		j.Map.Map(kv.Key, kv.Value, emitter)

		// Stop reading when end of inputSplit is reached
		pos := bytesRead
		if split.Size() > 0 && pos > split.Size() {
			break
		}
	}

	atomic.AddInt64(&j.bytesRead, bytesRead)

	return nil
}

// Logic for running a single reduce task
func (j *Job) runReducer(binID uint) error {
	//check if we can use a cacheFS instead
	var fs corfs.FileSystem = j.fileSystem
	if j.cacheSystem != nil {
		fs = j.cacheSystem
	}

	// Determine the intermediate data files this reducer is responsible for
	path := fs.Join(j.outputPath, fmt.Sprintf("map-bin%d-*", binID))
	files, err := fs.ListFiles(path)
	if err != nil {
		return err
	}

	// Open emitter for output data
	path = j.fileSystem.Join(j.outputPath, fmt.Sprintf("output-part-%d", binID))
	emitWriter, err := j.fileSystem.OpenWriter(path)
	defer emitWriter.Close()
	if err != nil {
		return err
	}

	data := make(map[string][]string, 0)
	var bytesRead int64

	for _, file := range files {
		reader, err := fs.OpenReader(file.Name, 0)
		bytesRead += file.Size
		if err != nil {
			return err
		}

		// Feed intermediate data into reducers
		decoder := json.NewDecoder(reader)
		for decoder.More() {
			var kv keyValue
			if err := decoder.Decode(&kv); err != nil {
				return err
			}

			if _, ok := data[kv.Key]; !ok {
				data[kv.Key] = make([]string, 0)
			}

			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
		reader.Close()

		// Delete intermediate map data
		if j.config.Cleanup {
			err := fs.Delete(file.Name)
			if err != nil {
				log.Error(err)
			}
		}
	}

	var waitGroup sync.WaitGroup
	sem := semaphore.NewWeighted(10)

	emitter := newReducerEmitter(emitWriter)
	for key, values := range data {
		sem.Acquire(context.Background(), 1)
		waitGroup.Add(1)
		go func(key string, values []string) {
			defer sem.Release(1)

			keyChan := make(chan string)
			keyIter := newValueIterator(keyChan)

			go func() {
				defer waitGroup.Done()
				j.Reduce.Reduce(key, keyIter, emitter)
			}()

			for _, value := range values {
				// Pass current value to the appropriate key channel
				keyChan <- value
			}
			close(keyChan)
		}(key, values)
	}

	waitGroup.Wait()

	atomic.AddInt64(&j.bytesWritten, emitter.bytesWritten())
	atomic.AddInt64(&j.bytesRead, bytesRead)

	return nil
}

// inputSplits calculates all input files' inputSplits.
// inputSplits also determines and saves the number of intermediate bins that will be used during the shuffle.
func (j *Job) inputSplits(inputs []string, maxSplitSize int64) []inputSplit {
	files := make([]string, 0)
	for _, inputPath := range inputs {
		fileInfos, err := j.fileSystem.ListFiles(inputPath)
		if err != nil {
			log.Warn(err)
			continue
		}

		for _, fInfo := range fileInfos {
			files = append(files, fInfo.Name)
		}
	}

	splits := make([]inputSplit, 0)
	var totalSize int64
	for _, inputFileName := range files {
		fInfo, err := j.fileSystem.Stat(inputFileName)
		if err != nil {
			log.Warnf("Unable to load input file: %s (%s)", inputFileName, err)
			continue
		}

		totalSize += fInfo.Size
		splits = append(splits, splitInputFile(fInfo, maxSplitSize)...)
	}
	if len(files) > 0 {
		log.Debugf("Average split size: %s bytes", humanize.Bytes(uint64(totalSize)/uint64(len(splits))))
	}

	j.intermediateBins = uint(float64(totalSize/j.config.ReduceBinSize) * 1.25)
	if j.intermediateBins == 0 {
		j.intermediateBins = 1
	}

	return splits
}

func (j *Job) done() {
	if j.activationLog != nil {
		close(j.activationLog)
	}
	j.wg.Wait()
}

func (j *Job) writeActivationLog() {

	logName := fmt.Sprintf("%s_%s.csv",
		viper.GetString("logName"),
		time.Now().Format("2006_01_02"))

	if viper.IsSet("logDir") {
		logName = filepath.Join(viper.GetString("logDir"), logName)
	} else if dir := os.Getenv("CORRAL_LOGDIR"); dir != "" {
		logName = filepath.Join(dir, logName)
	}

	logFile, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("failed to open activation log @ %s - %f", logName, err)
		return
	}
	writer := bufio.NewWriter(logFile)
	logWriter := csv.NewWriter(writer)

	//write header
	err = logWriter.Write([]string{
		"JId", "CId", "HId", "RId", "CStart", "EStart", "EEnd", "Read", "Written",
		"CMEM", "CMBS", "CRBS", "CSP", "CMC", "CTO",
	})
	if err != nil {
		log.Errorf("failed to open activation log @ %s - %f", logName, err)
		return
	}
	j.wg.Add(1)
	for task := range j.activationLog {

		err = logWriter.Write([]string{
			task.JId,
			task.CId,
			task.HId,
			task.RId,
			strconv.FormatInt(task.CStart, 10),
			strconv.FormatInt(task.EStart, 10),
			strconv.FormatInt(task.EEnd, 10),
			strconv.Itoa(task.BytesRead),
			strconv.Itoa(task.BytesWritten),
			strconv.FormatInt(viper.GetInt64("lambdaMemory"), 10),
			strconv.FormatInt(viper.GetInt64("mapBinSize"), 10),
			strconv.FormatInt(viper.GetInt64("reduceBinSize"), 10),
			strconv.FormatInt(viper.GetInt64("splitSize"), 10),
			strconv.FormatInt(viper.GetInt64("maxConcurrency"), 10),
			strconv.FormatInt(viper.GetInt64("lambdaTimeout"), 10),
		})
		if err != nil {
			log.Debugf("failed to write %+v - %f", task, err)
		}
		logWriter.Flush()
	}
	err = logFile.Close()
	log.Info("written metrics")
	j.wg.Done()
}

//needs to run in a process
func (j *Job) CollectMetrics() {
	j.activationLog = make(chan taskResult)
	j.wg = sync.WaitGroup{}
	j.writeActivationLog()

}

// NewJob creates a new job from a Mapper and Reducer.
func NewJob(mapper Mapper, reducer Reducer) *Job {
	job := &Job{
		Map:    mapper,
		Reduce: reducer,
		config: &config{},
	}

	return job
}
