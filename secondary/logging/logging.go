package logging

import "io"
import "os"
import "fmt"
import "strings"
import "time"
import "bytes"
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
	// Timing utility
	Timer(format string, v ...interface{}) Ender
	// Debugging messages. Not logged by default
	Debugf(format string, v ...interface{})
	// Program execution tracing. Not logged by default
	Tracef(format string, v ...interface{})
	// Get stack trace
	StackTrace() string
	// Call and print the stringer if debugging enabled
	LazyDebug(fn func() string)
	// Call and print the stringer if tracing enabled
	LazyTrace(fn func() string)
	
	// TODO remove this
	Printf(format string, v ...interface{})
}

// Timer interface
type Ender interface {
	// Stop and log timing
	End()
}

func (t LogLevel) String() string {
	switch t {
	case Silent:
		return "SILENT"
	case Fatal:
		return "FATAL"
	case Error:
		return "ERROR"
	case Warn:
		return "WARN"
	case Info:
		return "INFO"
	case Timing:
		return "TIMING"
	case Debug:
		return "DEBUG"
	case Trace:
		return "TRACE"
	default:
		return "INFO"
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

type Destination struct {
	level LogLevel
	dest  *l.Logger
}

type StopClock struct {
	name    string
	comment string
	start   time.Time
	log     *Destination
}

// Messages administrator should eventually see.
func (log *Destination) Warnf(format string, v ...interface{}) {
	log.printf(Warn, format, v...)
}

// Errors that caused problems in execution logic.
func (log *Destination) Errorf(format string, v ...interface{}) {
	log.printf(Error, format, v...)
}

// Fatal messages are to be logged prior to exiting due to errors.
func (log *Destination) Fatalf(format string, v ...interface{}) {
	log.printf(Fatal, format, v...)
}

// Info messages are those that are logged but not expected to be read.
func (log *Destination) Infof(format string, v ...interface{}) {
	log.printf(Info, format, v...)
}

// Function timing. Can be used as:
//    defer Time("Lets time this function").End()
//     ... function to be timed
// or
//    timer := Timer("For vbucket %d mutation %d", vbid, seq)
//     ... lines to be timed
//    timer.End()
//
func (log *Destination) Timer(format string, v ...interface{}) Ender {
	return log.timer(1, format, v...)
}

// Debug messages to help analyze problem. Default off.
func (log *Destination) Debugf(format string, v ...interface{}) {
	log.printf(Debug, format, v...)
}

// Execution trace showing the program flow. Default off.
func (log *Destination) Tracef(format string, v ...interface{}) {
	log.printf(Trace, format, v...)
}

// SetLogLevel sets current log level
func (log *Destination) SetLogLevel(to LogLevel) {
	log.level = to
}

// Get stack trace
func (log *Destination) StackTrace() string {
	return log.getStackTrace(2, debug.Stack())
}

// Run function only if output will be logged at debug level
func (log *Destination) LazyDebug(fn func() string) {
	if log.level >= Debug {
		log.Debugf("%s", fn())
	}
}

// Run function only if output will be logged at trace level
func (log *Destination) LazyTrace(fn func() string) {
	if log.level >= Trace {
		log.Debugf("%s", fn())
	}
}


// Internal functions
func (log *Destination) printf(at LogLevel, format string, v ...interface{}) {
	if log.level >= at {
		log.dest.Printf("["+at.String()+"] "+format, v...)
	}
}

func (log *Destination) getStackTrace(skip int, stack []byte) string {
	var buf bytes.Buffer
	lines := strings.Split(string(stack), "\n")
	for _, call := range lines[skip*2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}

func (watch *StopClock) End() {
	elapsed := time.Since(watch.start).Nanoseconds()
	watch.log.printf(Timing, "%s %.1f μs - %s", watch.name, float64(elapsed)/1000, watch.comment)
}

func (log *Destination) timer(skip int, format string, v ...interface{}) Ender {
	if log.level < Timing {
		return &dummyWatch
	}
	pc, file, _, ok := runtime.Caller(skip + 1)
	fn := runtime.FuncForPC(pc)
	if !ok || fn == nil {
		return &dummyWatch
	}
	name := fmt.Sprintf("%s:%s", filepath.Base(file), fn.Name())
	comment := fmt.Sprintf(format, v...)
	return &StopClock{name, comment, time.Now(), log}
}

// No op clock
var dummyWatch = dummyClock{}

type dummyClock struct {
}

func (_ *dummyClock) End() {
}

// The default logger
var SystemLogger Destination

func init() {
	dest := l.New(os.Stdout, "", l.Lmicroseconds)
	SystemLogger = Destination{Info, dest}
}

// SetLogWriter sets a new default destination
func SetLogWriter(w io.Writer) {
	syslog := l.New(w, "", l.Lmicroseconds)
	SystemLogger = Destination{Info, syslog}
}

//
// A set of convenience methods to log to default logger
// See correspond methods on Destination for details
//
func Warnf(format string, v ...interface{}) {
	SystemLogger.Warnf(format, v...)
}

// Errorf to log message and warning messages will be logged.
func Errorf(format string, v ...interface{}) {
	SystemLogger.Errorf(format, v...)
}

// Fatalf to log message and warning messages will be logged.
func Fatalf(format string, v ...interface{}) {
	SystemLogger.Fatalf(format, v...)
}

// Infof to log message at info level.
func Infof(format string, v ...interface{}) {
	SystemLogger.Infof(format, v...)
}

// Debugf to log message at info level.
func Debugf(format string, v ...interface{}) {
	SystemLogger.Debugf(format, v...)
}

// Tracef to log message at info level.
func Tracef(format string, v ...interface{}) {
	SystemLogger.Tracef(format, v...)
}

// StackTrace prints current stack at specified log level
func StackTrace() string {
	return SystemLogger.getStackTrace(2, debug.Stack())
}

// Timing utility function
func Timer(format string, v ...interface{}) Ender {
	return SystemLogger.timer(1, format, v...)
}

// SetLogLevel sets current log level
func SetLogLevel(to LogLevel) {
	SystemLogger.SetLogLevel(to)
}

// Run function only if output will be logged at debug level
func LazyDebug(fn func() string) {
	SystemLogger.LazyDebug(fn)
}

// Run function only if output will be logged at trace level
func LazyTrace(fn func() string) {
	SystemLogger.LazyTrace(fn)
}
