package corfs

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	lru "github.com/hashicorp/golang-lru"
	"github.com/mattetti/filebuffer"
)

var validMinioSchemes = map[string]bool{
	"minio": true,
}

// S3FileSystem abstracts AWS S3 as a filesystem
type MinioFileSystem struct {
	s3Client    *s3.S3
	objectCache *lru.Cache
}

func parseMinioUrl(uri string) (*url.URL, error) {
	parsed, err := parseURIWithMap(uri, validMinioSchemes)
	if err != nil {
		return nil, err
	}
	parsed.Scheme = "minio"

	return parsed, nil
}

// ListFiles lists files that match pathGlob.
func (s *MinioFileSystem) ListFiles(pathGlob string) ([]FileInfo, error) {
	s3Files := make([]FileInfo, 0)

	parsed, err := parseMinioUrl(pathGlob)
	if err != nil {
		return nil, err
	}

	baseURI := parsed.Path
	if globRegex.MatchString(parsed.Path) {
		baseURI = globRegex.FindStringSubmatch(parsed.Path)[1]
	}

	var dirGlob string
	if !strings.HasSuffix(pathGlob, "/") {
		dirGlob = pathGlob + "/*"
	} else {
		dirGlob = pathGlob + "*"
	}

	params := &s3.ListObjectsInput{
		Bucket: aws.String(parsed.Hostname()),
		Prefix: aws.String(baseURI),
	}

	objectPrefix := fmt.Sprintf("%s://%s/", parsed.Scheme, parsed.Hostname())
	err = s.s3Client.ListObjectsPages(params,
		func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				fullPath := objectPrefix + *object.Key

				dirMatch, _ := filepath.Match(dirGlob, fullPath)
				pathMatch, _ := filepath.Match(pathGlob, fullPath)
				if !(dirMatch || pathMatch) {
					continue
				}

				s3Files = append(s3Files, FileInfo{
					Name: fullPath,
					Size: *object.Size,
				})
				s.objectCache.Add(fullPath, object)
			}
			return true
		})

	return s3Files, err
}

// OpenReader opens a reader to the file at filePath. The reader
// is initially seeked to "startAt" bytes into the file.
func (s *MinioFileSystem) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	parsed, err := parseMinioUrl(filePath)
	if err != nil {
		return nil, err
	}

	objStat, err := s.Stat(filePath)
	if err != nil {
		return nil, err
	}

	reader := &s3Reader{
		client:    s.s3Client,
		bucket:    parsed.Hostname(),
		key:       parsed.Path,
		offset:    startAt,
		chunkSize: 20 * 1024 * 1024, // 20 Mb chunk size
		totalSize: objStat.Size,
	}
	err = reader.loadNextChunk()
	return reader, err
}

// OpenWriter opens a writer to the file at filePath.
func (s *MinioFileSystem) OpenWriter(filePath string) (io.WriteCloser, error) {
	parsed, err := parseMinioUrl(filePath)
	if err != nil {
		return nil, err
	}

	writer := &s3Writer{
		client:         s.s3Client,
		bucket:         parsed.Hostname(),
		key:            parsed.Path,
		buf:            filebuffer.New(nil),
		complatedParts: []*s3.CompletedPart{},
	}
	err = writer.Init()
	return writer, err
}

// Stat returns information about the file at filePath.
func (s *MinioFileSystem) Stat(filePath string) (FileInfo, error) {
	if object, exists := s.objectCache.Get(filePath); exists {
		return FileInfo{
			Name: filePath,
			Size: *object.(*s3.Object).Size,
		}, nil
	}

	parsed, err := parseMinioUrl(filePath)
	if err != nil {
		return FileInfo{}, err
	}

	params := &s3.ListObjectsInput{
		Bucket: aws.String(parsed.Hostname()),
		Prefix: aws.String(parsed.Path),
	}
	result, err := s.s3Client.ListObjects(params)
	if err != nil {
		return FileInfo{}, err
	}

	for _, object := range result.Contents {
		if *object.Key == parsed.Path {
			s.objectCache.Add(filePath, object)
			return FileInfo{
				Name: filePath,
				Size: *object.Size,
			}, nil
		}
	}

	return FileInfo{}, errors.New("No file with given filename")
}

// Init initializes the filesystem.
func (s *MinioFileSystem) Init() error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")

	var endpoint string
	if host := os.Getenv("MINIO_HOST"); host != "" {
		endpoint = host
	} else if host := os.Getenv("__OW_MINIO_HOST"); host != "" {
		endpoint = host
	} else if endpoint == "" {
		endpoint = viper.GetString("minioHost")
	} else if endpoint == "" {
		log.Error("could not minio determine endpoint")
		return fmt.Errorf("MINIO_ENDPOINT not set in env %+v", os.Environ())
	}

	// Configure to use MinIO Server
	s3Config := &aws.Config{
		Credentials: credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvProvider{},
			&PrefixEnvProvider{
				prefix: "__OW_",
				extraKeys: map[string]string{
					"id":     "MINIO_USER",
					"secret": "MINIO_KEY",
				},
			},
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     viper.GetString("minioUser"),
					SecretAccessKey: viper.GetString("minioKey"),
				},
			},
		}),
		Endpoint:         &endpoint,
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	if log.IsLevelEnabled(log.DebugLevel) {
		s3Config.WithCredentialsChainVerboseErrors(true)
	}

	//fail fast!
	newSession := session.Must(session.NewSession(s3Config))
	s.s3Client = s3.New(newSession)

	s.objectCache, _ = lru.New(10000)

	return nil
}

// Delete deletes the file at filePath.
func (s *MinioFileSystem) Delete(filePath string) error {
	parsed, err := parseMinioUrl(filePath)
	if err != nil {
		return err
	}

	params := &s3.DeleteObjectInput{
		Bucket: aws.String(parsed.Hostname()),
		Key:    aws.String(parsed.Path),
	}
	_, err = s.s3Client.DeleteObject(params)
	return err
}

// Join joins file path elements
func (s *MinioFileSystem) Join(elem ...string) string {
	stripped := make([]string, len(elem))
	for i, str := range elem {
		if strings.HasPrefix(str, "/") {
			str = str[1:]
		}
		if strings.HasSuffix(str, "/") && i != len(elem)-1 {
			str = str[:len(str)-1]
		}
		stripped[i] = str
	}
	return strings.Join(stripped, "/")
}

// Join joins file path elements
func (s *MinioFileSystem) Split(path string) []string {
	return strings.Split(path, "/")
}

// A EnvProvider retrieves credentials from the environment variables of the
// running process. Environment credentials never expire.
//
// Environment variables used:
//
// * Access Key ID:     [PREFIX]+AWS_ACCESS_KEY_ID or [PREFIX]+AWS_ACCESS_KEY
//
// * Secret Access Key: [PREFIX]+AWS_SECRET_ACCESS_KEY or [PREFIX]+AWS_SECRET_KEY
type PrefixEnvProvider struct {
	retrieved bool
	prefix    string
	extraKeys map[string]string
}

// NewEnvCredentials returns a pointer to a new Credentials object
// wrapping the environment variable provider.
func NewPrefixEnvCredentials(prefix string) *credentials.Credentials {
	return credentials.NewCredentials(&PrefixEnvProvider{prefix: prefix})
}

// Retrieve retrieves the keys from the environment.
func (e *PrefixEnvProvider) Retrieve() (credentials.Value, error) {
	e.retrieved = false

	ids := []string{e.prefix + "AWS_ACCESS_KEY_ID", e.prefix + "AWS_ACCESS_KEY"}
	secrets := []string{e.prefix + "AWS_SECRET_ACCESS_KEY", e.prefix + "AWS_SECRET_KEY"}
	if e.extraKeys != nil {
		if key, ok := e.extraKeys["id"]; ok {
			ids = append(ids, key)
			ids = append(ids, e.prefix+key)
		}
		if key, ok := e.extraKeys["secret"]; ok {
			secrets = append(secrets, key)
			secrets = append(secrets, e.prefix+key)
		}
	}

	id := lookupEnvFromKeys(ids)
	secret := lookupEnvFromKeys(secrets)

	if id == "" {
		return credentials.Value{ProviderName: credentials.EnvProviderName}, credentials.ErrAccessKeyIDNotFound
	}

	if secret == "" {
		return credentials.Value{ProviderName: credentials.EnvProviderName}, credentials.ErrSecretAccessKeyNotFound
	}

	e.retrieved = true
	return credentials.Value{
		AccessKeyID:     id,
		SecretAccessKey: secret,
		SessionToken:    os.Getenv(e.prefix + "AWS_SESSION_TOKEN"),
		ProviderName:    credentials.EnvProviderName,
	}, nil
}

// IsExpired returns if the credentials have been retrieved.
func (e *PrefixEnvProvider) IsExpired() bool {
	return !e.retrieved
}

func lookupEnvFromKeys(keys []string) string {
	for _, key := range keys {
		_id := os.Getenv(key)
		if _id != "" {
			return _id
		}
	}
	return ""
}
