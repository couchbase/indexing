package common

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Error, Warning, Fatal are always logged
const (
	// LogLevelInfo log messages for info
	LogLevelInfo int = iota + 1
	// LogLevelDebug log messages for info and debug
	LogLevelDebug
	// LogLevelTrace log messages info, debug and trace
	LogLevelTrace
)

var logLevel int
var logFile io.Writer = os.Stdout
var logger *log.Logger

func init() {
	logger = log.New(logFile, "", log.Lmicroseconds)
}

// LogLevel returns current log level
func LogLevel() int {
	return logLevel
}

// LogIgnore to ignore all log messages.
func LogIgnore() {
	logger = log.New(ioutil.Discard, "", log.Lmicroseconds)
}

// LogEnable to enable / re-enable log output.
func LogEnable() {
	logger = log.New(logFile, "", log.Lmicroseconds)
}

// SetLogLevel sets current log level
func SetLogLevel(level int) {
	logLevel = level
}

// SetLogWriter sets output file for log messages
func SetLogWriter(w io.Writer) {
	logger = log.New(w, "", log.Lmicroseconds)
	logFile = w
}

//-------------------------------
// Warning, Error, Fatal messages
//-------------------------------

// Warnf similar to fmt.Printf
func Warnf(format string, v ...interface{}) {
	logger.Printf("[WARN] "+format, v...)
}

// Errorf similar to fmt.Printf
func Errorf(format string, v ...interface{}) {
	logger.Printf("[ERROR] "+format, v...)
}

// Fatalf similar to fmt.Fatalf
func Fatalf(format string, v ...interface{}) {
	logger.Printf("[FATAL] "+format, v...)
}

//------------------------
// Informational debugging
//------------------------

// Infof if logLevel >= Info
func Infof(format string, v ...interface{}) {
	if logLevel >= LogLevelInfo {
		logger.Printf("[INFO] "+format, v...)
	}
}

//----------------
// Basic debugging
//----------------

// Debugf if logLevel >= Debug
func Debugf(format string, v ...interface{}) {
	if logLevel >= LogLevelDebug {
		logger.Printf("[DEBUG] "+format, v...)
	}
}

//----------------------
// Trace-level debugging
//----------------------

// Tracef if logLevel >= Trace
func Tracef(format string, v ...interface{}) {
	if logLevel >= LogLevelTrace {
		logger.Printf("[TRACE] "+format, v...)
	}
}
