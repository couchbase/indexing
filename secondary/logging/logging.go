package logging

import "io"
import "os"
import "fmt"
import "strings"
import "time"
import "bytes"
import "strconv"
import "runtime"
import "runtime/debug"
import "path/filepath"
import l "log"

// Log levels
type LogLevel int16

const (
	Silent LogLevel = iota
	Fatal
	Error
	Warn
	Info
	Timing
	Debug
	Trace
)

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
	// Get stack trace
	StackTrace() string
	// Timing utility
	Timer(format string, v ...interface{}) Ender
	// Debugging messages
	Debugf(format string, v ...interface{})
	// Program execution
	Tracef(format string, v ...interface{})
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

type filterMap map[string]LogLevel

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
	filters   filterMap
}

type stopClock struct {
	comment string
	skip    int
	start   time.Time
	log     *destination
}

func (log *destination) Warnf(format string, v ...interface{}) {
	log.printf(Warn, 1, format, v...)
}

// Errors that caused problems in execution logic.
func (log *destination) Errorf(format string, v ...interface{}) {
	log.printf(Error, 1, format, v...)
}

// Fatal messages are to be logged prior to exiting due to errors.
func (log *destination) Fatalf(format string, v ...interface{}) {
	log.printf(Fatal, 1, format, v...)
}

// Info messages are those that are logged but not expected to be read.
func (log *destination) Infof(format string, v ...interface{}) {
	log.printf(Info, 1, format, v...)
}

// Function timing. Use as:
//    defer Time("Waiting for backfill").End()
//     ... function to be timed
// or
//    timer := Timer("For vbucket %d mutation %d", vbid, seq)
//     ... lines to be timed
//    timer.End()
//
func (log *destination) Timer(format string, v ...interface{}) Ender {
	return log.timer(1, format, v...)
}

// Debug messages to help analyze problem. Default off.
func (log *destination) Debugf(format string, v ...interface{}) {
	log.printf(Debug, 1, format, v...)
}

// Execution trace showing the program flow. Default off.
func (log *destination) Tracef(format string, v ...interface{}) {
	log.printf(Trace, 1, format, v...)
}

// Set the base log level. Filters are cleared.
func (log *destination) SetLogLevel(to LogLevel) {
	log.baselevel = to
	added := make(filterMap)
	log.filters = added
}

// Get stack trace
func (log *destination) StackTrace() string {
	return log.getStackTrace(2, debug.Stack())
}

// Run function only if output will be logged at debug level
func (log *destination) LazyDebug(fn func() string) {
	if log.isEnabled(Debug, 1) {
		log.printf(Debug, 1, "%s", fn())
	}
}

// Run function only if output will be logged at trace level
func (log *destination) LazyTrace(fn func() string) {
	if log.isEnabled(Trace, 1) {
		log.printf(Trace, 1, "%s", fn())
	}
}

// Override log level for a file and/or line.
// Location is specified as file.go or file.go:22
func (log *destination) AddFilter(loc string, to LogLevel) {
	// infrequent, so clone to avoid locks
	added := make(filterMap)
	for k, v := range log.filters {
		added[k] = v
	}
	added[loc] = to
	log.filters = added
}

// Stop the running timer and print timing
func (watch *stopClock) End() {
	elapsed := time.Since(watch.start).Nanoseconds()
	watch.log.printf(Timing, watch.skip, "%.1f μs - %s", float64(elapsed)/1000, watch.comment)
}

// Clear all overrides
func (log *destination) ClearFilters() {
	// infrequent, so clone to avoid locks
	added := make(filterMap)
	log.filters = added
}

// Check if enabled
func (log *destination) isEnabled(at LogLevel, skip int) bool {
	// normal production case
	if len(log.filters) == 0 {
		return log.baselevel >= at
	}

	// unusual case, perhaps troubleshooting
	_, file, line, _ := runtime.Caller(skip + 1)
	base := filepath.Base(file)
	olvl, present := log.filters[base]
	if present {
		return olvl >= at
	}
	name := base + ":" + strconv.Itoa(line)
	olvl, present = log.filters[name]
	if present {
		return olvl >= at
	}

	return false
}

func (log *destination) printf(at LogLevel, skip int, format string, v ...interface{}) {
	if log.isEnabled(at, skip+1) {
		log.target.Printf("["+at.String()+"] "+format, v...)
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

func (log *destination) timer(skip int, format string, v ...interface{}) Ender {
	if !log.isEnabled(Timing, skip) {
		return emptyclock
	}
	comment := fmt.Sprintf(format, v...)
	return &stopClock{comment: comment, skip: skip, start: time.Now(), log: log}
}

// No op clock
var emptyclock = &emptyClock{}

type emptyClock struct{}

func (_ *emptyClock) End() {
}

// The default logger
var SystemLogger destination

func init() {
	dest := l.New(os.Stdout, "", l.Lmicroseconds)
	SystemLogger = destination{baselevel: Info, target: dest, filters: make(filterMap)}
}

// SetLogWriter sets a new default destination
func SetLogWriter(w io.Writer) {
	dest := l.New(w, "", l.Lmicroseconds)
	SystemLogger = destination{baselevel: Info, target: dest, filters: make(filterMap)}
}

//
// A set of convenience methods to log to default logger
// See correspond methods on destination for details
//
func Warnf(format string, v ...interface{}) {
	SystemLogger.printf(Warn, 1, format, v...)
}

// Errorf to log message and warning messages will be logged.
func Errorf(format string, v ...interface{}) {
	SystemLogger.printf(Error, 1, format, v...)
}

// Fatalf to log message and warning messages will be logged.
func Fatalf(format string, v ...interface{}) {
	SystemLogger.printf(Fatal, 1, format, v...)
}

// Infof to log message at info level.
func Infof(format string, v ...interface{}) {
	SystemLogger.printf(Info, 1, format, v...)
}

// Debugf to log message at info level.
func Debugf(format string, v ...interface{}) {
	SystemLogger.printf(Debug, 1, format, v...)
}

// Tracef to log message at info level.
func Tracef(format string, v ...interface{}) {
	SystemLogger.printf(Trace, 1, format, v...)
}

// StackTrace prints current stack at specified log level
func StackTrace() string {
	return SystemLogger.getStackTrace(2, debug.Stack())
}

// Timing utility function
func Timer(format string, v ...interface{}) Ender {
	return SystemLogger.timer(2, format, v...)
}

// SetLogLevel sets current log level
func SetLogLevel(to LogLevel) {
	SystemLogger.SetLogLevel(to)
}

// Override log level for a file and/or line.
func AddFilter(loc string, to LogLevel) {
	SystemLogger.AddFilter(loc, to)
}

// Run function only if output will be logged at debug level
func LazyDebug(fn func() string) {
	if SystemLogger.isEnabled(Debug, 1) {
		SystemLogger.printf(Debug, 1, "%s", fn())
	}
}

// Run function only if output will be logged at trace level
func LazyTrace(fn func() string) {
	if SystemLogger.isEnabled(Trace, 1) {
		SystemLogger.printf(Trace, 1, "%s", fn())
	}
}
