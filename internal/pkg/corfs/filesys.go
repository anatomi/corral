package corfs

import (
	log "github.com/sirupsen/logrus"
	"io"
	"strings"
)

// FileSystemType is an identifier for supported FileSystems
type FileSystemType int

// Identifiers for supported FileSystemTypes
const (
	Local FileSystemType = iota
	S3
	MINIO
)

// FileSystem provides the file backend for MapReduce jobs.
// Input data is read from a file system. Intermediate and output data
// is written to a file system.
// This is abstracted to allow remote filesystems like S3 to be supported.
type FileSystem interface {
	ListFiles(pathGlob string) ([]FileInfo, error)
	Stat(filePath string) (FileInfo, error)
	OpenReader(filePath string, startAt int64) (io.ReadCloser, error)
	OpenWriter(filePath string) (io.WriteCloser, error)
	Delete(filePath string) error
	Join(elem ...string) string
	Init() error
}

// FileInfo provides information about a file
type FileInfo struct {
	Name string // file path
	Size int64  // file size in bytes
}

// InitFilesystem intializes a filesystem of the given type
func InitFilesystem(fsType FileSystemType) FileSystem {
	var fs FileSystem
	switch fsType {
	case Local:
		log.Debug("using local fs")
		fs = &LocalFileSystem{}
	case S3:
		log.Debug("using s3 fs")
		fs = &S3FileSystem{}
	case MINIO:
		log.Debug("using minio fs")
		fs = &MinioFileSystem{}

	}

	fs.Init()
	return fs
}

// InferFilesystem initializes a filesystem by inferring its type from
// a file address.
// For example, locations starting with "s3://" will resolve to an S3
// filesystem.
func InferFilesystem(location string) FileSystem {
	var fs FileSystem
	if strings.HasPrefix(location, "s3://") {
		log.Debug("using s3 fs")
		fs = &S3FileSystem{}
	} else if strings.HasPrefix(location, "minio://") {
		log.Debug("using minio fs")
		fs = &MinioFileSystem{}
	} else {
		log.Debug("using local fs")
		fs = &LocalFileSystem{}
	}

	fs.Init()
	return fs
}
