package corfs

import (
	"fmt"
	"net/url"
	"strings"
)

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func parseURIWithMap(uri string, validS3Schemes map[string]bool) (*url.URL, error) {
	parsed, err := url.Parse(uri)

	if _, ok := validS3Schemes[parsed.Scheme]; !ok {
		return nil, fmt.Errorf("Invalid s3 scheme: '%s'", parsed.Scheme)
	}

	// if !strings.Contains(parsed.Path, "/") {
	// 	return nil, fmt.Errorf("Invalid s3 url: '%s'", uri)
	// }

	if strings.HasPrefix(parsed.Path, "/") {
		parsed.Path = parsed.Path[1:]
	}

	return parsed, err
}
