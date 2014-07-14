package common

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

var buffer *bytes.Buffer

func init() {
	buffer = bytes.NewBuffer([]byte{})
	logger = log.New(buffer, "", log.Lmicroseconds)
}

func TestLogLevel(t *testing.T) {
	SetLogLevel(LogLevelDebug)
	if LogLevel() != LogLevelDebug {
		t.Errorf("SetLogLevel() not respected")
	}
	SetLogLevel(0)
}

func TestLogIgnore(t *testing.T) {
	Warnf("test")
	if s := string(buffer.Bytes()); strings.Contains(s, "test") == false {
		t.Errorf("Warnf() failed %v", s)
	}
	LogIgnore()
	Warnf("test")
	if s := string(buffer.Bytes()); s == "" {
		t.Errorf("Warnf() failed %v", s)
	}
	LogEnable()
}

func TestLogLevelDefault(t *testing.T) {
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
	} else if strings.Contains(s, "info") == true {
		t.Errorf("Infof() failed %v", s)
	} else if strings.Contains(s, "debug") == true {
		t.Errorf("Debugf() failed %v", s)
	} else if strings.Contains(s, "trace") == true {
		t.Errorf("Tracef() failed %v", s)
	}
	SetLogWriter(os.Stdout)
}

func TestLogLevelInfo(t *testing.T) {
	SetLogWriter(buffer)
	SetLogLevel(LogLevelInfo)
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
	SetLogWriter(buffer)
	SetLogLevel(LogLevelDebug)
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
	SetLogWriter(buffer)
	SetLogLevel(LogLevelTrace)
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
