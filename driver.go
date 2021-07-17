package corral

import (
	"context"
	"fmt"
	"github.com/ISE-SMILE/corral/internal/pkg/corcache"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/spf13/viper"

	"golang.org/x/sync/semaphore"

	log "github.com/sirupsen/logrus"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/ISE-SMILE/corral/internal/pkg/corfs"

	flag "github.com/spf13/pflag"
)

var src rand.Source

func init() {
	rand.Seed(time.Now().UnixNano())
	src = rand.NewSource(time.Now().UnixNano())
}

// Driver controls the execution of a MapReduce Job
type Driver struct {
	jobs      []*Job
	config    *config
	executor  executor
	cache     corcache.CacheSystem
	runtimeID string
	Start     time.Time

	currentJob  int
	Runtime     time.Duration

	lastOutputs []string
}

func (d *Driver) CurrentJob() *Job  {
	if d.currentJob >= 0 && d.currentJob < len(d.jobs){
		return d.jobs[d.currentJob]
	}
	return nil
}

// config configures a Driver's execution of jobs
type config struct {
	Inputs          []string
	SplitSize       int64
	MapBinSize      int64
	ReduceBinSize   int64
	MaxConcurrency  int
	WorkingLocation string
	Cleanup         bool
	Cache  			corcache.CacheSystemType
}

func newConfig() *config {
	loadConfig() // Load viper config from settings file(s) and environment

	// Register command line flags
	flag.Parse()
	viper.BindPFlags(flag.CommandLine)

	return &config{
		Inputs:          []string{},
		SplitSize:       viper.GetInt64("splitSize"),
		MapBinSize:      viper.GetInt64("mapBinSize"),
		ReduceBinSize:   viper.GetInt64("reduceBinSize"),
		MaxConcurrency:  viper.GetInt("maxConcurrency"),
		WorkingLocation: viper.GetString("workingLocation"),
		Cleanup:         viper.GetBool("cleanup"),
	}
}

// Option allows configuration of a Driver
type Option func(*config)

// NewDriver creates a new Driver with the provided job and optional configuration
func NewDriver(job *Job, options ...Option) *Driver {
	d := &Driver{
		jobs:      []*Job{job},
		executor:  &localExecutor{time.Now()},
		runtimeID: randomName(),
		Start:     time.Now(),
	}

	c := newConfig()
	for _, f := range options {
		f(c)
	}

	if c.SplitSize > c.MapBinSize {
		log.Warn("Configured Split Size is larger than Map Bin size")
		c.SplitSize = c.MapBinSize
	}

	d.config = c
	log.Debugf("Loaded config: %#v", c)

	if c.Cache != corcache.NoCache {
		cache,err := corcache.NewCacheSystem(c.Cache)
		if err != nil {
			log.Debugf("failed to init cache, %+v",err)
			panic(err)
		} else {
			log.Infof("using cache %s",c.Cache)
		}
		d.cache = cache
	}

	return d
}

// NewMultiStageDriver creates a new Driver with the provided jobs and optional configuration
func NewMultiStageDriver(jobs []*Job, options ...Option) *Driver {
	driver := NewDriver(nil, options...)
	driver.jobs = jobs
	return driver
}

// WithSplitSize sets the SplitSize of the Driver
func WithSplitSize(s int64) Option {
	return func(c *config) {
		c.SplitSize = s
	}
}

// WithMapBinSize sets the MapBinSize of the Driver
func WithMapBinSize(s int64) Option {
	return func(c *config) {
		c.MapBinSize = s
	}
}

// WithReduceBinSize sets the ReduceBinSize of the Driver
func WithReduceBinSize(s int64) Option {
	return func(c *config) {
		c.ReduceBinSize = s
	}
}

// WithWorkingLocation sets the location and filesystem backend of the Driver
func WithWorkingLocation(location string) Option {
	return func(c *config) {
		c.WorkingLocation = location
	}
}

// WithInputs specifies job inputs (i.e. input files/directories)
func WithInputs(inputs ...string) Option {
	return func(c *config) {
		c.Inputs = append(c.Inputs, inputs...)
	}
}

func WithLocalMemoryCache() Option {
	return func(c *config) {
		c.Cache = corcache.Local
	}
}

func WithRedisBackedCache() Option {
	return func(c *config) {
		c.Cache = corcache.Redis
	}
}

func (d *Driver) GetFinalOutputs() []string{
	return d.lastOutputs
}

func (d *Driver) DownloadAndRemove(inputs []string, dest string) error {
	fs := corfs.InferFilesystem(inputs[0])

	files := make([]string,0)
	for _,input := range inputs {
		list,err := fs.ListFiles(input)
		if err != nil {
			return err
		}
		for _,f := range list {
			files = append(files,f.Name)
		}

	}

	log.Infof("found %d files to download",len(files))
	bar := pb.New(len(files)).Prefix("DownloadAndRemove").Start()
	for _,path := range files {
		wr,err := os.OpenFile(filepath.Join(dest,path),os.O_CREATE|os.O_RDWR,0664)
		if err != nil {
			return err
		}
		r,err := fs.OpenReader(path,0)
		if err != nil {
			return err
		}
		_,err = io.Copy(wr,r)
		if err != nil {
			return err
		}

		err = fs.Delete(path)
		if err != nil {
			return err
		}

		bar.Increment()
	}
	bar.Finish()



	return nil
}

func (d *Driver) runMapPhase(job *Job, jobNumber int, inputs []string) {
	inputSplits := job.inputSplits(inputs, d.config.SplitSize)
	if len(inputSplits) == 0 {
		log.Warnf("No input splits")
		return
	}
	log.Debugf("Number of job input splits: %d", len(inputSplits))

	inputBins := packInputSplits(inputSplits, d.config.MapBinSize)
	log.Debugf("Number of job input bins: %d", len(inputBins))
	bar := pb.New(len(inputBins)).Prefix("Map").Start()

	//tell the platfrom how may invocations we plan on dooing
	err := d.executor.HintSplits(uint(len(inputBins)))
	if err != nil{
		log.Warn("failed to hint platfrom, expect perfromance degredations")
		log.Debugf("hint error:%+v",err)
	}

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(d.config.MaxConcurrency))
	for binID, bin := range inputBins {
		//XXX: binID casted to uint
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bID uint, b []inputSplit) {
			defer wg.Done()
			defer sem.Release(1)
			defer bar.Increment()
			err := d.executor.RunMapper(job, jobNumber, bID, b)
			if err != nil {
				log.Errorf("Error when running mapper %d: %s", bID, err)
			}
		}(uint(binID), bin)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runReducePhase(job *Job, jobNumber int) {
	var wg sync.WaitGroup
	bar := pb.New(int(job.intermediateBins)).Prefix("Reduce").Start()

	//tell the platfrom how may invocations we plan on dooing
	err := d.executor.HintSplits(job.intermediateBins)
	if err != nil{
		log.Warn("failed to hint platfrom, expect perfromance degredations")
		log.Debugf("hint error:%+v",err)
	}

	for binID := uint(0); binID < job.intermediateBins; binID++ {
		wg.Add(1)
		go func(bID uint) {
			defer wg.Done()
			defer bar.Increment()
			err := d.executor.RunReducer(job, jobNumber, bID)
			if err != nil {
				log.Errorf("Error when running reducer %d: %s", bID, err)
			}
		}(binID)
	}
	wg.Wait()
	bar.Finish()
}

func (d *Driver) runOnCloudPlatfrom() bool {
	if runningInLambda() {
		log.Debug(">>>Running on AWS Lambda>>>")
		//XXX this is sub-optimal (in case we need to init this struct we have to come up with a different strategy ;)
		(&lambdaExecutor{}).Start(d)
		return true
	}
	if runningInWhisk() {
		log.Debug(">>>Running on OpenWhisk or IBM>>>")
		(&whiskExecutor{}).Start(d)
		return true
	}

	return false
}

// run starts the Driver
func (d *Driver) run() {
	if d.runOnCloudPlatfrom() {
		log.Warn("Running on FaaS runtime and Returned, this is bad!")
		os.Exit(-10)
	}

	//TODO: do preflight checks e.g. check if in/out is accassible...
	//TODO introduce interface for deploy/undeploy
	if lBackend, ok := d.executor.(platform); ok {
		err := lBackend.Deploy(d)
		if err != nil{
			panic(err)
		}
	}

	if d.cache != nil{
		err := d.cache.Deploy()
		if err != nil{
			log.Errorf("failed to deploy cache, %+v",err)
		}

		err = d.cache.Init()
		if err != nil{
			log.Errorf("failed to initilized cache, %+v",err)
		}
	}




	if len(d.config.Inputs) == 0 {
		log.Error("No inputs!")
		log.Error(os.Environ())
		return
	}

	if rlh := viper.GetString("remoteLoggingHost");rlh != ""{
		log.Debugf("using remote logging host: %s for functions",rlh)
	}

	inputs := d.config.Inputs
	for idx, job := range d.jobs {
		if viper.GetBool("verbose") || *verbose {
			log.Debugf("collecting job metrics")
			go job.CollectMetrics()
		}
		// Initialize job filesystem
		job.fileSystem = corfs.InferFilesystem(inputs[0])

		//set cache system based on driver
		job.cacheSystem = d.cache

		jobWorkingLoc := d.config.WorkingLocation
		log.Infof("Starting job%d (%d/%d)", idx, idx+1, len(d.jobs))

		if len(d.jobs) > 1 {
			jobWorkingLoc = job.fileSystem.Join(jobWorkingLoc, fmt.Sprintf("job%d", idx))
		}
		job.outputPath = jobWorkingLoc

		*job.config = *d.config
		d.runMapPhase(job, idx, inputs)
		d.runReducePhase(job, idx)

		// Set inputs of next job to be outputs of current job
		inputs = []string{job.fileSystem.Join(jobWorkingLoc, "output-*")}
		d.lastOutputs = inputs

		log.Infof("Job %d - Total Bytes Read:\t%s", idx, humanize.Bytes(uint64(job.bytesRead)))
		log.Infof("Job %d - Total Bytes Written:\t%s", idx, humanize.Bytes(uint64(job.bytesWritten)))

		//check if we need to flush the intermedate data to disk
		if viper.GetBool("durable") && !viper.GetBool("cleanup") {
			if d.cache != nil {
				//we dont need to wait for this to finish, this might also take a while ...
				go func(cache corcache.CacheSystem,fs corfs.FileSystem){
					err := cache.Flush(fs)

					if err != nil {
						log.Errorf("failed to flush cache to fs, %+v",err)
					}
				}(d.cache,job.fileSystem)
			}
		}

		//clear cache
		if viper.GetBool("cleanup") && d.cache != nil {
			err := d.cache.Clear()
			if err != nil{
				log.Warnf("failed to cleanup cache, %+v",err)
			}
		}

		job.done()
	}
}
var backendFlag = flag.StringP("backend", "b", "", "Define backend [local,lambda,whisk] - default local")

var outputDir = flag.StringP("out", "o", "", "Output `directory` (can be local or in S3)")
var memprofile = flag.String("memprofile", "", "Write memory profile to `file`")
var verbose = flag.BoolP("verbose", "v", false, "Output verbose logs")

var undeploy = flag.Bool("undeploy", false, "Undeploy the Lambda function and IAM permissions without running the driver")
// Main starts the Driver, running the submitted jobs.
func (d *Driver) Main() {

	//log.SetLevel(log.DebugLevel)
	if viper.GetBool("verbose") || *verbose {
		log.SetLevel(log.DebugLevel)
	}

	if *undeploy {
		//TODO: this is a shitty interface/abstraction
		if backendFlag == nil {
			panic("missing backend flag!")
		}
		//TODO: modify the constants
		var backend platform
		if *backendFlag == "lambda" {
			backend = newLambdaExecutor(viper.GetString("lambdaFunctionName"))
		} else if *backendFlag == "whisk" {
			backend = newWhiskExecutor(viper.GetString("lambdaFunctionName"))
		}

		err := backend.Undeploy()
		if err != nil{
			log.Errorf("failed to undeploy, you are on youre own %s",err)
		}


		if d.cache != nil {
			err := d.cache.Undeploy()
			if err != nil {
				log.Warnf("failed to undeploy cache, you are on youre own %s", err)
			}
		}
		return
	}

	d.config.Inputs = append(d.config.Inputs, flag.Args()...)
	if backendFlag != nil {
		if *backendFlag == "lambda" {
			d.executor = newLambdaExecutor(viper.GetString("lambdaFunctionName"))
		} else if *backendFlag == "whisk" {
			d.executor = newWhiskExecutor(viper.GetString("lambdaFunctionName"))
		}
	}

	if *outputDir != "" {
		d.config.WorkingLocation = *outputDir
	}

	start := time.Now()
	d.run()
	end := time.Now()
	fmt.Printf("Job Execution Time: %s\n", end.Sub(start))
	d.Runtime = end.Sub(start)
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}

