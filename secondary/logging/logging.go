package logging

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	l "log"
	"runtime"
)

// Log levels
type LogLevel int16

const (
	Silent LogLevel = iota
	Fatal
	Error
	Warn
	Info
	Verbose
	Timing
	Debug
	Trace
)

const udtag_begin = "<ud>"
const udtag_end = "</ud>"

// Logger interface
type Logger interface {
	// Warnings, logged by default.
	Warnf(format string, v ...interface{})
	// Errors, logged by default.
	Errorf(format string, v ...interface{})
	// Fatal errors. Will not terminate execution.
	Fatalf(format string, v ...interface{})
	// Informational messages.
	Infof(format string, v ...interface{})
	// Verbose messages like request logging
	Verbosef(format string, v ...interface{})
	// Get stack trace
	StackTrace() string
	// Timing utility
	Timer(format string, v ...interface{}) Ender
	// Debugging messages
	Debugf(format string, v ...interface{})
	// Program execution
	Tracef(format string, v ...interface{})
	// Call and print the stringer if verbose enabled
	LazyVerbose(fn func() string)
	// Call and print the stringer if debugging enabled
	LazyDebug(fn func() string)
	// Call and print the stringer if tracing enabled
	LazyTrace(fn func() string)
}

// Timer interface
type Ender interface {
	// Stop and log timing
	End()
}

//
// Implementation
//

// Messages administrator should eventually see.
func (t LogLevel) String() string {
	switch t {
	case Silent:
		return "Silent"
	case Fatal:
		return "Fatal"
	case Error:
		return "Error"
	case Warn:
		return "Warn"
	case Info:
		return "Info"
	case Verbose:
		return "Verbose"
	case Timing:
		return "Timing"
	case Debug:
		return "Debug"
	case Trace:
		return "Trace"
	default:
		return "Info"
	}
}

func Level(s string) LogLevel {
	switch strings.ToUpper(s) {
	case "SILENT":
		return Silent
	case "FATAL":
		return Fatal
	case "ERROR":
		return Error
	case "WARN":
		return Warn
	case "INFO":
		return Info
	case "VERBOSE":
		return Verbose
	case "TIMING":
		return Timing
	case "DEBUG":
		return Debug
	case "TRACE":
		return Trace
	default:
		return Info
	}
}

type destination struct {
	baselevel LogLevel
	target    *l.Logger
}

func (log *destination) Warnf(format string, v ...interface{}) {
	log.printf(Warn, format, v...)
}

// Errors that caused problems in execution logic.
func (log *destination) Errorf(format string, v ...interface{}) {
	log.printf(Error, format, v...)
}

// Fatal messages are to be logged prior to exiting due to errors.
func (log *destination) Fatalf(format string, v ...interface{}) {
	log.printf(Fatal, format, v...)
}

// Info messages are those that are logged but not expected to be read.
func (log *destination) Infof(format string, v ...interface{}) {
	log.printf(Info, format, v...)
}

// Used for logging additional information that are not logged by info level
// This may slightly impact performance
func (log *destination) Verbosef(format string, v ...interface{}) {
	log.printf(Verbose, format, v...)
}

// Debug messages to help analyze problem. Default off.
func (log *destination) Debugf(format string, v ...interface{}) {
	log.printf(Debug, format, v...)
}

// Execution trace showing the program flow. Default off.
func (log *destination) Tracef(format string, v ...interface{}) {
	log.printf(Trace, format, v...)
}

// Set the base log level
func (log *destination) SetLogLevel(to LogLevel) {
	log.baselevel = to
}

// Get stack trace
func (log *destination) StackTrace() string {
	return log.getStackTrace(2, debug.Stack())
}

// StackTraceAll prints all goroutine stacks at specified log level
func (log *destination) StackTraceAll() string {
	buf := make([]byte, 500000)
	n := runtime.Stack(buf, true)
	return string(buf[0:n])
}

// Get profiling info
func Profile(port string, endpoints ...string) string {
	if strings.HasPrefix(port, ":") {
		port = port[1:]
	}
	var buf bytes.Buffer
	for _, endpoint := range endpoints {
		addr := fmt.Sprintf("http://localhost:%s/debug/pprof/%s?debug=1", port, endpoint)
		resp, err := http.Get(addr)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		buf.Write(body)
	}
	return buf.String()
}

// Dump profiling info periodically
func PeriodicProfile(level LogLevel, port string, endpoints ...string) {
	tick := time.NewTicker(5 * time.Minute)
	go func() {
		for {
			select {
			case <-tick.C:
				if SystemLogger.IsEnabled(level) {
					SystemLogger.printf(level, "%v", Profile(port, endpoints...))
				}
			}
		}
	}()
}

// Run function only if output will be logged at debug level
func (log *destination) LazyDebug(fn func() string) {
	if log.IsEnabled(Debug) {
		log.printf(Debug, "%s", fn())
	}
}

// Run function only if output will be logged at verbose level
func (log *destination) LazyVerbose(fn func() string) {
	if log.IsEnabled(Verbose) {
		log.printf(Verbose, "%s", fn())
	}
}

// Run function only if output will be logged at trace level
func (log *destination) LazyTrace(fn func() string) {
	if log.IsEnabled(Trace) {
		log.printf(Trace, "%s", fn())
	}
}

// Check if enabled
func (log *destination) IsEnabled(at LogLevel) bool {
	return log.baselevel >= at
}

func (log *destination) printf(at LogLevel, format string, v ...interface{}) {
	if log.IsEnabled(at) {
		ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
		log.target.Printf(ts+" ["+at.String()+"] "+format, v...)
	}
}

func (log *destination) getStackTrace(skip int, stack []byte) string {
	var buf bytes.Buffer
	lines := strings.Split(string(stack), "\n")
	for _, call := range lines[skip*2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}

// The default logger
var SystemLogger destination

func init() {
	dest := l.New(os.Stdout, "", 0)
	SystemLogger = destination{baselevel: Info, target: dest}
}

// SetLogWriter sets a new default destination
func SetLogWriter(w io.Writer) {
	dest := l.New(w, "", 0)
	SystemLogger = destination{baselevel: Info, target: dest}
}

// A set of convenience methods to log to default logger
// See correspond methods on destination for details
func Warnf(format string, v ...interface{}) {
	SystemLogger.printf(Warn, format, v...)
}

// Errorf to log message and warning messages will be logged.
func Errorf(format string, v ...interface{}) {
	SystemLogger.printf(Error, format, v...)
}

// Fatalf to log message and warning messages will be logged.
func Fatalf(format string, v ...interface{}) {
	SystemLogger.printf(Fatal, format, v...)
}

// Infof to log message at info level.
func Infof(format string, v ...interface{}) {
	SystemLogger.printf(Info, format, v...)
}

// Verbosef to log message at verbose level.
func Verbosef(format string, v ...interface{}) {
	SystemLogger.printf(Verbose, format, v...)
}

// Debugf to log message at info level.
func Debugf(format string, v ...interface{}) {
	SystemLogger.printf(Debug, format, v...)
}

// Tracef to log message at info level.
func Tracef(format string, v ...interface{}) {
	SystemLogger.printf(Trace, format, v...)
}

// StackTrace prints current stack at specified log level
func StackTrace() string {
	return SystemLogger.getStackTrace(2, debug.Stack())
}

// StackTraceAll prints all goroutine stacks at specified log level
func StackTraceAll() string {
	return SystemLogger.StackTraceAll()
}

// Set the base log level
func SetLogLevel(to LogLevel) {
	SystemLogger.SetLogLevel(to)
}

// Check if logging is enabled
func IsEnabled(lvl LogLevel) bool {
	return SystemLogger.IsEnabled(lvl)
}

// Run function only if output will be logged at verbose level
func LazyVerbose(fn func() string) {
	if SystemLogger.IsEnabled(Verbose) {
		SystemLogger.printf(Verbose, "%s", fn())
	}
}

// Run function only if output will be logged at debug level
func LazyDebug(fn func() string) {
	if SystemLogger.IsEnabled(Debug) {
		SystemLogger.printf(Debug, "%s", fn())
	}
}

// Run function only if output will be logged at trace level
func LazyTrace(fn func() string) {
	if SystemLogger.IsEnabled(Trace) {
		SystemLogger.printf(Trace, "%s", fn())
	}
}

// Run function only if output will be logged at verbose level
// Only %v is allowable in format string
func LazyVerbosef(fmt string, fns ...func() string) {
	if SystemLogger.IsEnabled(Verbose) {
		snippets := make([]interface{}, len(fns))
		for i, fn := range fns {
			snippets[i] = fn()
		}
		SystemLogger.printf(Verbose, fmt, snippets...)
	}
}

// Run function only if output will be logged at debug level
// Only %v is allowable in format string
func LazyDebugf(fmt string, fns ...func() string) {
	if SystemLogger.IsEnabled(Debug) {
		snippets := make([]interface{}, len(fns))
		for i, fn := range fns {
			snippets[i] = fn()
		}
		SystemLogger.printf(Debug, fmt, snippets...)
	}
}

// Run function only if output will be logged at trace level
// Only %v is allowable in format string
func LazyTracef(fmt string, fns ...func() string) {
	if SystemLogger.IsEnabled(Trace) {
		snippets := make([]interface{}, len(fns))
		for i, fn := range fns {
			snippets[i] = fn()
		}
		SystemLogger.printf(Trace, fmt, snippets...)
	}
}

// Tag user data in default format
func TagUD(arg interface{}) interface{} {
	return fmt.Sprintf("%s(%v)%s", udtag_begin, arg, udtag_end)
}

// Tag user data in string format
func TagStrUD(arg interface{}) interface{} {
	return fmt.Sprintf("%s(%s)%s", udtag_begin, arg, udtag_end)
}

type TimedStackTrace struct {
	last   uint64
	period uint64
}

func NewTimedStackTrace(period time.Duration) *TimedStackTrace {
	return &TimedStackTrace{
		period: uint64(period),
	}
}

// Log only once over a TimedLogger.Period of time
func (tl *TimedStackTrace) TryLogStackTrace(level LogLevel, errStr string) {
	new := uint64(time.Now().UnixNano())
	old := atomic.LoadUint64(&tl.last)
	if new-old > tl.period && atomic.CompareAndSwapUint64(&tl.last, old, new) {
		SystemLogger.printf(level, "%v: %v", errStr, StackTrace())
	}
}

// TimedNStackTraces is not Concurrent safe
type TimedNStackTraces struct {
	maxSize int
	period  time.Duration
	errList []string
	curIdx  int
	lgrMap  map[string]*TimedStackTrace
}

func NewTimedNStackTraces(maxSize int, period time.Duration) *TimedNStackTraces {
	return &TimedNStackTraces{
		maxSize: maxSize,
		period:  period,
		errList: make([]string, maxSize),
		lgrMap:  make(map[string]*TimedStackTrace),
	}
}

func (tl *TimedNStackTraces) TryLogStackTrace(level LogLevel, errStr string) {
	if errStr == "" {
		return
	}

	lgr, ok := tl.lgrMap[errStr]
	if !ok {
		// cleanup old one if available
		oldErrStr := tl.errList[tl.curIdx]
		if oldErrStr != "" {
			delete(tl.lgrMap, oldErrStr)
		}

		// create new logger
		tl.errList[tl.curIdx] = errStr
		lgr = NewTimedStackTrace(tl.period)
		tl.lgrMap[errStr] = lgr

		// update curIdx
		tl.curIdx = tl.curIdx + 1
		if tl.curIdx >= tl.maxSize {
			tl.curIdx = 0
		}
	}

	lgr.TryLogStackTrace(level, errStr)
}
