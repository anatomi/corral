package corbuild

import (
	"archive/zip"
	"bytes"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// crossCompile builds the current directory as a lambda package.
// It returns the location of a built binary file.
func crossCompile(binName string) (string, error) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", err
	}

	outputPath := filepath.Join(tmpDir, binName)

	args := []string{
		"build",
		"-o", outputPath,
		"-ldflags", "-s -w",
		".",
	}
	cmd := exec.Command("go", args...)

	cmd.Env = append(os.Environ(), "GOOS=linux")
	cmd.Env = append(os.Environ(), "GOARCH=amd64")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")

	combinedOut, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s\n%s", err, combinedOut)
	}

	return outputPath, nil
}

// buildPackage builds the current directory as a lambda package.
// It returns a byte slice containing a compressed binary that can be upload to lambda.
func BuildPackage(mainFnName string) ([]byte, error) {
	log.Info("Building function")
	binFile, err := crossCompile("lambda_artifact")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(filepath.Dir(binFile)) // Remove temporary binary file

	log.Debug("Opening recompiled binary to be zipped")
	binReader, err := os.Open(binFile)
	if err != nil {
		return nil, err
	}

	zipBuf := new(bytes.Buffer)
	archive := zip.NewWriter(zipBuf)
	header := &zip.FileHeader{
		Name:           mainFnName,
		ExternalAttrs:  (0777 << 16), // File permissions
		CreatorVersion: (3 << 8),     // Magic number indicating a Unix creator
	}

	log.Debug("Adding binary to zip archive")
	writer, err := archive.CreateHeader(header)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(writer, binReader)
	if err != nil {
		return nil, err
	}

	//In case we are building an openwhisk package...
	data := []byte("openwhisk/action-golang-v1.15\n")
	header = &zip.FileHeader{
		Name:               "exec.env",
		UncompressedSize64: uint64(len(data)),
		Method:             zip.Deflate,
	}

	writer, err = archive.CreateHeader(header)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(writer, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	binReader.Close()
	archive.Close()

	log.Debugf("Final zipped function binary size: %s", humanize.Bytes(uint64(len(zipBuf.Bytes()))))
	data = zipBuf.Bytes()
	if log.IsLevelEnabled(log.DebugLevel){
		f,err := ioutil.TempFile("","")
		if err == nil{
			_,_ = f.Write(data)
			_ = f.Close()
			log.Debugf("deployment package at %s",f.Name())
		}
	}

	return data, nil
}

func InjectConfiguration(env map[string]*string) {
	if host := viper.GetString("minioHost"); host != "" {
		env["MINIO_HOST"] = &host
	}

	if user := viper.GetString("minioUser"); user != "" {
		env["MINIO_USER"] = &user
	}

	if key := viper.GetString("minioKey"); key != "" {
		env["MINIO_KEY"] = &key
	}
}
