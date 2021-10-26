package corcache

import (
	"bytes"
	"fmt"
	"github.com/anatomi/corral/internal/pkg/corfs"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const sep = "_"

type LocalCache struct {
	size    uint64
	maxSize uint64
	pool    *sync.Map
}

func NewLocalInMemoryProvider(maxSize uint64) *LocalCache {
	return &LocalCache{
		size:    0,
		maxSize: maxSize,
		pool:    &sync.Map{},
	}
}

type WriteCloser struct {
	*bytes.Buffer
	lmp *LocalCache
}

func (w *WriteCloser) Write(p []byte) (n int, err error) {
	return w.Buffer.Write(p)
}

func (w *WriteCloser) Close() error {
	atomic.AddUint64(&w.lmp.size, uint64(w.Buffer.Len()))
	return nil
}

func (l *LocalCache) ListFiles(path string) ([]corfs.FileInfo, error) {
	//XXX: glob not working well

	if l.pool == nil {
		return nil, fmt.Errorf("cache is closed or failed to init")
	}
	files := make([]corfs.FileInfo, 0)
	scan := func(key interface{}, raw interface{}) bool {
		//that semse risky
		file := key.(string)
		buf := raw.(*bytes.Buffer)

		if ok, _ := filepath.Match(path, file); ok {
			files = append(files, corfs.FileInfo{
				Name: file,
				Size: int64(buf.Len()),
			})
		} else if strings.HasSuffix(path, "*") {
			if strings.HasPrefix(file, path[:len(path)-1]) {
				files = append(files, corfs.FileInfo{
					Name: file,
					Size: int64(buf.Len()),
				})
			}
		}
		return true
	}

	l.pool.Range(scan)
	return files, nil
}

func (l *LocalCache) Stat(path string) (corfs.FileInfo, error) {
	if l.pool == nil {
		return corfs.FileInfo{}, fmt.Errorf("cache is closed or failed to init")
	}

	if raw, ok := l.pool.Load(path); ok {
		buf := raw.(*bytes.Buffer)
		return corfs.FileInfo{
			Name: path,
			Size: int64(buf.Len()),
		}, nil
	} else {
		return corfs.FileInfo{}, fmt.Errorf("file not availible")
	}
}

func (l *LocalCache) OpenReader(path string, startAt int64) (io.ReadCloser, error) {
	if l.pool == nil {
		return nil, fmt.Errorf("cache is closed or failed to init")
	}
	if raw, ok := l.pool.Load(path); ok {
		buf := raw.(*bytes.Buffer)
		cbuf := bytes.NewBuffer(buf.Bytes())
		if startAt > 0 {
			_ = cbuf.Next(int(startAt))
		}
		return io.NopCloser(cbuf), nil
	} else {
		return nil, fmt.Errorf("file not availible")
	}
}

func (l *LocalCache) OpenWriter(path string) (io.WriteCloser, error) {
	if l.pool == nil {
		return nil, fmt.Errorf("cache is closed or failed to init")
	}
	if l.size < l.maxSize {
		var buf *bytes.Buffer
		if raw, ok := l.pool.Load(path); ok {
			buf = raw.(*bytes.Buffer)
		} else {
			buf = bytes.NewBuffer([]byte{})
			l.pool.Store(path, buf)
		}
		return &WriteCloser{buf, l}, nil
	} else {
		return nil, fmt.Errorf("not enught space %d of %d bytes used", l.size, l.maxSize)
	}
}

func (l *LocalCache) Delete(path string) error {
	if l.pool == nil {
		return fmt.Errorf("cache is closed or failed to init")
	}
	if raw, ok := l.pool.Load(path); ok {
		buf := raw.(*bytes.Buffer)
		l.size = l.size - uint64(buf.Len())
		l.pool.Delete(path)
		return nil
		//} else {
		//	files,err := l.ListFiles(path)
		//	if err != nil {
		//		return fmt.Errorf("failed to list files to delete %+v",err)
		//	}
		//	if len(files) == 0 {
		//		return fmt.Errorf("file dose not exsist")
		//	}
		//
		//
		//	for _, f := range files {
		//		err = l.Delete(f.Name)
		//		if err != nil {
		//			return err
		//		}
		//	}
		//	return nil

	} else {
		return fmt.Errorf("file dose not exsist")
	}

}

func (l *LocalCache) Join(elem ...string) string {
	return strings.Join(elem, sep)
}

func (l *LocalCache) Init() error {
	return nil
}

func (l *LocalCache) Deploy() error {
	return nil
}

func (l *LocalCache) Undeploy() error {
	l.size = 0
	l.pool = nil
	return nil
}

func (l *LocalCache) Flush(fs corfs.FileSystem) error {
	errors := make([]error, 0)

	scan := func(key, val interface{}) bool {
		path := key.(string)
		buf := val.(*bytes.Buffer)

		//TODO: we prop need to test this on win and lx also probl between each fs variant ...
		fsPath := fs.Join(l.Split(path)...)
		w, err := fs.OpenWriter(fsPath)
		if err != nil {
			errors = append(errors, err)
			return true
		}

		buf.WriteTo(w)
		w.Close()

		return true
	}

	l.pool.Range(scan)
	if len(errors) > 0 {
		return fmt.Errorf("got errors while flushing %+v", errors)
	}
	return nil
}

func (l *LocalCache) Clear() error {
	l.size = 0
	l.pool = &sync.Map{}
	return nil
}

func (l *LocalCache) Split(path string) []string {
	return strings.Split(path, sep)
}

func (l *LocalCache) FunctionInjector() CacheConfigIncector {
	return nil
}
