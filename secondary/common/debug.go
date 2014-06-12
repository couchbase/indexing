package common

import (
	"log"
	"os"
)

const (
	// LogLevelInfo log messages for info
	LogLevelInfo int = iota + 1
	// LogLevelDebug log messages for info and debug
	LogLevelDebug
	// LogLevelTrace log messages info, debug and trace
	LogLevelTrace
)

var logLevel int
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "", log.Lmicroseconds)
}

// LogLevel returns current log level
func LogLevel() int {
	return logLevel
}

// SetLogLevel sets current log level
func SetLogLevel(level int) {
	logLevel = level
}

//-----------------
// Warning messages
//-----------------

// Warn similar to fmt.Print
func Warn(v ...interface{}) {
	logger.Print(v...)
}

// Warnf similar to fmt.Printf
func Warnf(format string, v ...interface{}) {
	logger.Printf(format, v...)
}

// Warnln similar to fmt.Println.
func Warnln(v ...interface{}) {
	logger.Println(v...)
}

//------------------------
// Informational debugging
//------------------------

// Info if logLevel >= Info
func Info(v ...interface{}) {
	if logLevel >= LogLevelInfo {
		logger.Print(v...)
	}
}

// Infof if logLevel >= Info
func Infof(format string, v ...interface{}) {
	if logLevel >= LogLevelInfo {
		logger.Printf(format, v...)
	}
}

// Infoln if logLevel >= Info
func Infoln(v ...interface{}) {
	if logLevel >= LogLevelInfo {
		logger.Println(v...)
	}
}

//----------------
// Basic debugging
//----------------

// Debug if logLevel >= Debug
func Debug(v ...interface{}) {
	if logLevel >= LogLevelDebug {
		logger.Print(v...)
	}
}

// Debugf if logLevel >= Debug
func Debugf(format string, v ...interface{}) {
	if logLevel >= LogLevelDebug {
		logger.Printf(format, v...)
	}
}

// Debugln if logLevel >= Debug
func Debugln(v ...interface{}) {
	if logLevel >= LogLevelDebug {
		logger.Println(v...)
	}
}

//----------------------
// Trace-level debugging
//----------------------

// Trace if logLevel >= Trace
func Trace(v ...interface{}) {
	if logLevel >= LogLevelTrace {
		logger.Print(v...)
	}
}

// Tracef if logLevel >= Trace
func Tracef(format string, v ...interface{}) {
	if logLevel >= LogLevelTrace {
		logger.Printf(format, v...)
	}
}

// Traceln if logLevel >= Trace
func Traceln(v ...interface{}) {
	if logLevel >= LogLevelTrace {
		logger.Println(v...)
	}
}
