package logging

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

//
//
//
//
//
//
//
func LocationMarker() {
	Fatalf("twentyzero") // this must be on line 20 of this file!
	SystemLogger.Errorf("twentyone")
	Debugf("twentytwo")
}

var buffer *bytes.Buffer

func init() {
	buffer = bytes.NewBuffer([]byte{})
	buffer.Reset()
	SetLogWriter(buffer)
}

func TestLogIgnore(t *testing.T) {
	buffer.Reset()
	Warnf("test")
	if s := string(buffer.Bytes()); strings.Contains(s, "test") == false {
		t.Errorf("Warnf() failed %v", s)
	}
	SetLogLevel(Silent)
	Warnf("test")
	if s := string(buffer.Bytes()); s == "" {
		t.Errorf("Warnf() failed %v", s)
	}
	SetLogLevel(Info)
}

func TestLogLevelDefault(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	Warnf("warn")
	Errorf("error")
	Fatalf("fatal")
	Infof("info")
	Debugf("debug")
	Tracef("trace")
	s := string(buffer.Bytes())
	if strings.Contains(s, "warn") == false {
		t.Errorf("Warnf() failed %v", s)
	} else if strings.Contains(s, "error") == false {
		t.Errorf("Errorf() failed %v", s)
	} else if strings.Contains(s, "fatal") == false {
		t.Errorf("Fatalf() failed %v", s)
	} else if strings.Contains(s, "info") == false {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == true {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == true {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestLogLevelInfo(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Info)
	Warnf("warn")
	Infof("info")
	Debugf("debug")
	Tracef("trace")
	s := string(buffer.Bytes())
	if strings.Contains(s, "warn") == false {
		t.Errorf("Warnf() failed %v", s)
	} else if strings.Contains(s, "info") == false {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == true {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == true {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestLogLevelDebug(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Debug)
	Warnf("warn")
	Infof("info")
	Debugf("debug")
	Tracef("trace")
	s := string(buffer.Bytes())
	if strings.Contains(s, "warn") == false {
		t.Errorf("Warnf() failed %v", s)
	} else if strings.Contains(s, "info") == false {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == false {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == true {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestLogLevelTrace(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Trace)
	Warnf("warn")
	Infof("info")
	Debugf("debug")
	Tracef("trace")
	s := string(buffer.Bytes())
	if strings.Contains(s, "warn") == false {
		t.Errorf("Warnf() failed %v", s)
	} else if strings.Contains(s, "info") == false {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == false {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == false {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestLogLevels(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Error)
	Fatalf("allow_me_1")
	Infof("dont_allow")
	Errorf("allow_me_2")
	s := string(buffer.Bytes())
	if strings.Contains(s, "allow_me_1") == false {
		t.Errorf("Levels failed to allow %v", s)
	} else if strings.Contains(s, "dont_allow") == true {
		t.Errorf("Levels failed to disallow %v", s)
	} else if strings.Contains(s, "allow_me_2") == false {
		t.Errorf("Level scoping failed %v", s)
	}
}

func TestSystemLog(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Trace)
	sl := SystemLogger
	sl.Warnf("warn")
	sl.Infof("info")
	sl.Debugf("debug")
	sl.Tracef("trace")
	s := string(buffer.Bytes())
	if strings.Contains(s, "warn") == false {
		t.Errorf("Warnf() failed %v", s)
	} else if strings.Contains(s, "info") == false {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == false {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == false {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestTimingLogging(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Timing)
	TimeMeStyle1()
	if r, _ := regexp.Match(` 10[0-9]{5}\.[0-9] μs`, buffer.Bytes()); !r {
		t.Errorf("Timer failed %v", string(buffer.Bytes()))
	}
	buffer.Reset()
	TimeMeStyle2()
	if r, _ := regexp.Match(` 10[0-9]{5}\.[0-9] μs`, buffer.Bytes()); !r {
		t.Errorf("Timer failed %v", string(buffer.Bytes()))
	}
}

func TestStackTheTrace(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Error)
	StackMe()
	s := string(buffer.Bytes())
	if strings.Contains(s, "TestStackTheTrace: StackMe()") == false {
		t.Errorf("StackTrace failed, missing frames %v", s)
	}
	if strings.Contains(strings.ToLower(s), "stacktrace") == true {
		t.Errorf("StackTrace failed, too many frames %v", s)
	}
}

func TestLevels(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Error)
	LocationMarker()
	s := string(buffer.Bytes())
	if strings.Contains(s, "twentyzero") == false {
		t.Errorf("Location mark on on line 20: %v", s)
	}
	if strings.Contains(s, "twentyone") == false {
		t.Errorf("Location mark on on line 21: %v", s)
	}
	if strings.Contains(s, "twentytwo") == true {
		t.Errorf("Location mark on on line 21: %v", s)
	}
}

func TestFilters1(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Error)
	AddFilter("logging_test.go", Debug)
	LocationMarker()
	s := string(buffer.Bytes())
	if strings.Contains(s, "twentyzero") == false {
		t.Errorf("Location mark on on line 20: %v", s)
	}
	if strings.Contains(s, "twentyone") == false {
		t.Errorf("Location mark on on line 21: %v", s)
	}
	if strings.Contains(s, "twentytwo") == false {
		t.Errorf("Location mark on on line 22: %v", s)
	}
}

func TestFilters2(t *testing.T) {
	buffer.Reset()
	SetLogWriter(buffer)
	SetLogLevel(Error)
	AddFilter("logging_test.go:20", Silent)
	AddFilter("logging_test.go:22", Debug)
	LocationMarker()
	s := string(buffer.Bytes())
	if strings.Contains(s, "twentyzero") == true {
		t.Errorf("Location mark on on line 20: %v", s)
	}
	if strings.Contains(s, "twentyone") == true {
		t.Errorf("Location mark on on line 21: %v", s)
	}
	if strings.Contains(s, "twentytwo") == false {
		t.Errorf("Location mark on on line 22: %v", s)
	}
}

func TimeMeStyle1() {
	defer Timer("test").End()
	time.Sleep(1 * time.Second)
}

func TimeMeStyle2() {
	timer := Timer("test")
	time.Sleep(1 * time.Second)
	timer.End()
}

func StackMe() {
	st := StackTrace()
	SystemLogger.Errorf(st)
}
