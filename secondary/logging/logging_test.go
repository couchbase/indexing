package logging

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

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

func StackMe() {
	st := StackTrace()
	SystemLogger.Errorf(st)
}
