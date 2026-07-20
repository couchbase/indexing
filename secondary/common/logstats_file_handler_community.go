//go:build community

package common

import (
	"os"
	"path/filepath"

	"github.com/couchbase/logstats/logstats"
)

// LogStatsFileHandler is the community stub. It delegates to the standard
// logstats open/rotate logic
//
//	No encryption is applied in community builds.
type LogStatsFileHandler struct{}

func NewLogStatsFileHandler(_ func() (keyID string, key []byte), _ func(string) ([]byte, string)) *LogStatsFileHandler {
	return &LogStatsFileHandler{}
}

func (h *LogStatsFileHandler) DisableCompression() {}
func (h *LogStatsFileHandler) PauseRotation()      {}
func (h *LogStatsFileHandler) ResumeRotation()     {}

func (h *LogStatsFileHandler) Open(fileName string) (logstats.SyncWriteCloser, int, error) {
	if err := os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
		return nil, 0, err
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, int(fi.Size()), nil
}

func (h *LogStatsFileHandler) Rotate(fileName string, numFiles int) (logstats.SyncWriteCloser, int, error) {
	return logstats.RotateLogFile(fileName, numFiles, false)
}

func GetStatsLogFileKeyID(_ string) (string, error) {
	return "", nil
}

func IsStatsLogActiveFileEncrypted(_ string) bool {
	return false
}

func ReencryptStatsLogFiles(_ string, _ string, _ []string, _ string, _ []byte, _ func(string) ([]byte, string)) error {
	return nil
}

func DecryptStatsLogFiles(_ string, _ string, _ []string, _ func(string) ([]byte, string)) error {
	return nil
}
