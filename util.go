package corral

import (
	"bufio"
	"encoding/base64"
	"os"
	"strings"
)

func readUptime() string {
	file, err := os.Open("/proc/uptime")
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		parts := strings.Split(text, " ")
		return base64.StdEncoding.EncodeToString([]byte(parts[0]))
	}
	return ""
}