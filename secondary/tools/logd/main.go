package main

import "flag"
import "os"
import "log"
import "fmt"
import "time"
import "strings"
import "regexp"
import "io/ioutil"

var options struct {
	show []string
}

func argParse() []string {
	var show string

	flag.StringVar(&show, "show", "", "log lines to show")

	flag.Parse()

	options.show = strings.Split(show, ",")
	args := flag.Args()
	if len(args) == 0 {
		log.Fatalf("Please specify a log file!!")
	}
	// validate whether log files are preset
	for _, logfile := range args {
		if _, err := os.Stat(logfile); err != nil {
			log.Fatal(err)
		}
	}
	return args
}

func main() {
	args := argParse()
	analyseLog(args[0])
}

func analyseLog(logfile string) {
	var msg *LogMsg
	var skips []string

	lines := readLines(logfile)
	msgs := make([]*LogMsg, 0)
	skipped := make([]string, 0)
	for len(lines) > 0 {
		msg, skips, lines = newLogMsg(lines)
		if !msg.isEmpty() {
			msgs = append(msgs, msg)
		}
		skipped = append(skipped, skips...)
	}
	fmt.Printf("Number of messages: %d\n", len(msgs))
	fmt.Printf("Lines skipped: %d\n", len(skipped))
	for _, show := range options.show {
		switch show {
		case "skip":
			listLines(skipped)
		}
	}
}

// log line
type Line string

func (l Line) time() time.Time {
	t, err := time.Parse(time.StampMicro, string(l))
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func makefilter(all, any []*regexp.Regexp, pass bool) func(string) bool {
	return func(line string) bool {
		if all != nil {
			for _, re := range all {
				b := re.Match([]byte(line))
				if (pass && !b) || (!pass && b) {
					return false
				}
			}
			return true
		}
		for _, re := range any {
			b := re.Match([]byte(line))
			if (pass && b) || (!pass && !b) {
				return true
			}
		}
		return false
	}
}

func filter(fn func(string) bool, lines []string) []string {
	filtered := []string{}
	for _, line := range lines {
		if fn(line) {
			filtered = append(filtered, line)
		}
	}
	return filtered
}

func partition(fn func(string) bool, lines []string) ([]string, []string) {
	this, that := []string{}, []string{}
	for _, line := range lines {
		if fn(line) {
			this = append(this, line)
		} else {
			that = append(that, line)
		}
	}
	return this, that
}

func readLines(file string) []string {
	s, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	return strings.Split(string(s), "\n")
}

func listLines(lines []string) {
	for _, line := range lines {
		fmt.Println(line)
	}
}

// Validation on log lines.

func validate(lines []string) {
	validatePRAMRegistration(lines)
}

func validatePRAMRegistration(lines []string) {
	re, err := regexp.Compile(
		`registered.*` +
			`(/adminport/vbmapRequest|` +
			`/adminport/failoverLogRequest|` +
			`/adminport/mutationTopicRequest|` +
			`/adminport/restartVbucketsRequest|` +
			`/adminport/shutdownVbucketsRequest|` +
			`/adminport/addBucketsRequest|` +
			`/adminport/delBucketsRequest|` +
			`/adminport/addInstancesRequest|` +
			`/adminport/delInstancesRequest|` +
			`/adminport/repairEndpointsRequest|` +
			`/adminport/shutdownTopicRequest|` +
			`/adminport/stats|` +
			`/stats|` +
			`/settings)`)
	if err != nil {
		log.Fatal(err)
	}
	count := 0
	for _, line := range lines {
		if re.Match([]byte(line)) {
			count++
		}
	}
	if count != 14 {
		log.Fatalf("validaterPRAMRegistration(): %v", count)
	}
}

// parse feeds

type FeedLines struct {
	name      string
	crashed   []string
	stales    []string
	fatals    []string
	errors    []string
	warns     []string
	engines   []string
	endpoints map[string][]string // endpid -> lines
	byopaque  map[string][]string // opaque -> lines
}

var re_feed, _ = regexp.Compile(` FEED\[<=>([^\(]*)(`)
var re_feedcrashed, _ = regexp.Compile(` FEED\[<=>[^\]*\].*feed gen-server crashed`)
var re_feedstale, _ = regexp.Compile(` FEED\[<=>[^\]*\].*feed.*stale`)
var re_feedfatal, _ = regexp.Compile(`[Fatal].*FEED\[<=>[^\]*\].*feed.*stale`)
var re_feedwarn, _ = regexp.Compile(`[Warn].*FEED\[<=>[^\]*\].*feed.*stale`)
var re_feederror, _ = regexp.Compile(`[Error].*FEED\[<=>[^\]*\].*feed.*stale`)
var re_feedengns, _ = regexp.Compile(`[Error].*FEED\[<=>[^\]*\].*engine`)
var fmt_feedendps = `[Error].*ENDP\[<-[^#]*#%s]`

// log messages

var regs_timestamp = `^(\d\d:\d\d:\d\d.\d\d\d\d\d\d) `
var regs_level = `\[(Fatal|Error|Warn|Debug|Info|Trace)\] `
var regs_comp = `(PROJ|PRAM|FEED|KVDT|VBRT|ENDP)`
var re_goport, _ = regexp.Compile(`^\[goport\]`)
var re_basic, _ = regexp.Compile(regs_timestamp + regs_level + regs_comp)

type LogMsg struct {
	msg   string
	ts    string
	level string
	kind  string
}

func newLogMsg(lines []string) (msg *LogMsg, skipped, remlines []string) {
	if len(lines) == 0 {
		return nil, nil, nil
	}
	skipped = make([]string, 0)
	i := 0
	msg = &LogMsg{msg: "", kind: ""}
	for ; i < len(lines); i++ {
		line := lines[i]
		if msg.msg != "" && strings.HasPrefix(line, "    ") {
			msg.msg = msg.msg + "\n" + line
		} else if msg.msg != "" {
			return msg, skipped, lines[i:]
		} else {
			msg.msg = line
			ok := msg.basic()
			if !ok {
				msg.msg = ""
				line = strings.Trim(line, "\t \r\n")
				if line != "" {
					skipped = append(skipped, line)
				}
			}
		}
	}
	return msg, skipped, nil
}

func (msg *LogMsg) basic() bool {
	bs := []byte(msg.msg)
	if re_goport.Match(bs) {
		msg.kind = "goport"
	} else if m := re_basic.FindStringSubmatch(msg.msg); m != nil {
		msg.ts, msg.level, msg.kind = m[1], m[2], m[3]
	} else {
		return false
	}
	return true
}

func (msg *LogMsg) isGoport() bool {
	return msg.kind == "goport"
}

func (msg *LogMsg) isEmpty() bool {
	return msg.msg == ""
}

// operations on list of messages

type LogMsgs []*LogMsg

func (msgs LogMsgs) goports() []LogMsgs {
	if len(msgs) == 0 {
		return nil
	}
	blocks := make([]LogMsgs, 0)
	block := make(LogMsgs, 0)
	for _, msg := range msgs {
		if re_goport.Match([]byte(msg.msg)) {
			blocks = append(blocks, block)
			block = make(LogMsgs, 0)
		}
		block = append(block, msg)
	}
	if len(block) > 0 {
		blocks = append(blocks, block)
	}
	return blocks
}
