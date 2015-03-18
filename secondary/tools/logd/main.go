package main

import "flag"
import "os"
import "log"
import "fmt"
import "strings"
import "regexp"
import "io/ioutil"

var options struct {
	show    []string
	session int
}

func argParse() []string {
	var show string

	flag.StringVar(&show, "show", "", "log lines to show")
	flag.IntVar(&options.session, "session", 0, "session to analyse")

	flag.Parse()

	for _, s := range strings.Split(show, ",") {
		if options.show == nil {
			options.show = []string{}
		}
		options.show = append(options.show, strings.ToLower(s))
	}
	args := flag.Args()
	if len(args) == 0 {
		log.Fatalf("Please specify a log file!!")
	}

	// validate whether log files are present
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

	// convert lines to log-messages
	lines := readLines(logfile)
	msgs := make([]*LogMsg, 0)
	skipped := make([]string, 0)
	for len(lines) > 0 {
		msg, skips, lines = lines2msg(lines)
		if !msg.isEmpty() {
			msgs = append(msgs, msg)
		}
		skipped = append(skipped, skips...)
	}
	validate(msgs)

	// log messages to goport-sessions.
	sessions := getSessions(msgs)

	fmt.Printf("Number of messages: %d\n", len(msgs))
	fmt.Printf("Number of goport-sessions: %d\n", len(sessions))
	fmt.Printf("Lines skipped: %d\n", len(skipped))

	session := sessions[options.session]
	analyseSession(session)

	for _, show := range options.show {
		switch show {
		case "skip":
			listLines(skipped)
		}
	}
}

func analyseSession(session LogMsgs) {
	// gather requests
	requests := gatherRequests(session)
	for _, show := range options.show {
		var reqs Requests
		switch show {
		case "vbmaprequest":
			reqs = requests.vbmapRequests()
		case "failoverlog":
			reqs = requests.failoverLogRequests()
		case "mutationtopic":
			reqs = requests.mutationTopicRequests()
		case "restartvbuckets":
			reqs = requests.restartVbucketsRequests()
		case "shutdownvbuckets":
			reqs = requests.shutdownVbucketsRequests()
		case "addbuckets":
			reqs = requests.addBucketsRequests()
		case "delbuckets":
			reqs = requests.delBucketsRequests()
		case "addinstances":
			reqs = requests.addInstancesRequests()
		case "delinstances":
			reqs = requests.delInstancesRequests()
		case "repairendpoints":
			reqs = requests.repairEndpointsRequests()
		case "shutdowntopic":
			reqs = requests.shutdownTopics()
		}
		fmt.Printf("Total of %v requests for %v\n", len(reqs), show)
		for _, r := range reqs {
			r.printReq()
		}
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

//-------------
// log messages
//-------------

var re_goport, _ = regexp.Compile(
	`^\[goport\] (\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d) `)
var re_basic, _ = regexp.Compile(
	`^(\d\d:\d\d:\d\d.\d\d\d\d\d\d) ` +
		`\[(Fatal|Error|Warn|Debug|Info|Trace)\] ` +
		`(PROJ|PRAM|FEED|KVDT|VBRT|ENDP|DCPT).*( ##[0-9a-f]+ )?`)
var re_settings, _ = regexp.Compile(
	`^(\d\d:\d\d:\d\d.\d\d\d\d\d\d) \[(Info)\] New settings`)
var re_angio, _ = regexp.Compile(` (##[0-9a-f]+) `)
var re_reqType, _ = regexp.Compile(
	`(doVbmapRequest|` +
		`doFailoverLog|` +
		`doMutationTopic|` +
		`doRestartVbuckets|` +
		`doShutdownVbuckets|` +
		`doAddBuckets|` +
		`doDelBuckets|` +
		`doAddInstances|` +
		`doDelInstances|` +
		`doRepairEndpoints|` +
		`doShutdownTopic)\(\)`)

type LogMsg struct {
	msg      string
	ts       string
	level    string
	kind     string
	reqType  string
	feedName string
	angio    string
}

// convert one or more log lines into a log messages.
func lines2msg(lines []string) (msg *LogMsg, skipped, remlines []string) {
	if len(lines) == 0 {
		return nil, nil, nil
	}
	skipped = make([]string, 0)
	msg = &LogMsg{msg: "", kind: ""}
	ok, n := false, -1
	for i, line := range lines {
		if (msg.msg != "") && ((n > 0) || strings.HasPrefix(line, "    ")) {
			msg.msg = msg.msg + "\n" + line // amend with previous line
			n--

		} else if msg.msg != "" {
			return msg, skipped, lines[i:]

		} else {
			msg.msg = line
			if ok, n = msg.parseHeadLine(); !ok {
				msg.msg = ""
				if strings.Trim(line, "\t \r\n") != "" {
					skipped = append(skipped, line)
				}
			}
		}
	}
	return msg, skipped, nil
}

func (msg *LogMsg) parseHeadLine() (bool, int) {
	if m := re_goport.FindStringSubmatch(msg.msg); m != nil {
		msg.ts, msg.kind = m[1], "goport"
	} else if m := re_basic.FindStringSubmatch(msg.msg); m != nil {
		msg.ts, msg.level, msg.kind = m[1], m[2], m[3]
		if m = re_angio.FindStringSubmatch(msg.msg); m != nil {
			msg.angio = m[1]
		}
		if m = re_reqType.FindStringSubmatch(msg.msg); m != nil {
			msg.reqType = m[1]
		}
	} else if m := re_settings.FindStringSubmatch(msg.msg); m != nil {
		msg.ts, msg.level, msg.kind = m[1], m[2], "settings"
		return true, 1
	} else {
		return false, 0
	}
	return true, -1
}

func (msg *LogMsg) isGoport() bool {
	return msg.kind == "goport"
}

func (msg *LogMsg) isEmpty() bool {
	return msg.msg == ""
}

func (msg *LogMsg) hasPattern(arg interface{}) bool {
	var re *regexp.Regexp
	var err error
	if s, ok := arg.(string); ok {
		if re, err = regexp.Compile(s); err != nil {
			log.Fatal(err)
		}
	} else if re, ok = arg.(*regexp.Regexp); !ok {
		log.Fatalf("unexpected arg %T %v\n", arg, arg)
	}
	return re.Match([]byte(msg.msg))
}

func (msg *LogMsg) hasAngio(angio string) bool {
	return (angio != "") && (msg.angio == angio)
}

func (msg *LogMsg) isVbmapRequest() bool {
	return msg.reqType == "doVbmapRequest"
}

func (msg *LogMsg) isFailoverLog() bool {
	return msg.reqType == "doFailoverLog"
}

func (msg *LogMsg) isMutationTopic() bool {
	return msg.reqType == "doMutationTopic"
}

func (msg *LogMsg) isRestartVbuckets() bool {
	return msg.reqType == "doRestartVbuckets"
}

func (msg *LogMsg) isShutdownVbuckets() bool {
	return msg.reqType == "doShutdownVbuckets"
}

func (msg *LogMsg) isAddBuckets() bool {
	return msg.reqType == "doAddBuckets"
}

func (msg *LogMsg) isDelBuckets() bool {
	return msg.reqType == "doDelBuckets"
}

func (msg *LogMsg) isAddInstances() bool {
	return msg.reqType == "doAddInstances"
}

func (msg *LogMsg) isDelInstances() bool {
	return msg.reqType == "doDelInstances"
}

func (msg *LogMsg) isRepairEndpoints() bool {
	return msg.reqType == "doRepairEndpoints"
}

func (msg *LogMsg) isShutdownTopic() bool {
	return msg.reqType == "doShutdownTopic"
}

//---------
// sessions
//---------

type LogMsgs []*LogMsg

func getSessions(msgs []*LogMsg) []LogMsgs {
	if len(msgs) == 0 {
		return nil
	}
	sessions := make([]LogMsgs, 0)
	session := make(LogMsgs, 0)
	for _, msg := range msgs {
		if msg.kind == "goport" && len(session) > 0 {
			sessions = append(sessions, session)
			session = make(LogMsgs, 0)
		}
		session = append(session, msg)
	}
	if len(session) > 0 {
		sessions = append(sessions, session)
	}
	return sessions
}

//---------------
// track requests
//---------------

func gatherRequests(msgs LogMsgs) Requests {
	cache := map[string]bool{}
	requests := []*Request{}
	for len(msgs) > 0 {
		msg := msgs[0]
		if _, ok := cache[msg.angio]; msg.angio != "" && !ok {
			r := newRequest(msg)
			msgs = r.trackRequest(msg.angio, msgs)
			cache[msg.angio] = true
			requests = append(requests, r)
		} else {
			msgs = msgs[1:]
		}
	}
	return requests
}

//--------
// request
//--------

type Request struct {
	reqmsg *LogMsg
	typ    string
	msgs   []*LogMsg
}

func newRequest(reqmsg *LogMsg) *Request {
	return &Request{
		reqmsg: reqmsg,
		msgs:   make([]*LogMsg, 0),
	}
}

func (r *Request) trackRequest(angio string, msgs []*LogMsg) (rems []*LogMsg) {
	rems = make([]*LogMsg, 0)
	for _, msg := range msgs {
		if msg.hasAngio(angio) {
			r.msgs = append(r.msgs, msg)
		} else {
			rems = append(rems, msg)
		}
	}
	return rems
}

func (r *Request) printReq() {
	for _, msg := range r.msgs {
		fmt.Println(msg.msg)
	}
}

type Requests []*Request

func (rs Requests) vbmapRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isVbmapRequest() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) failoverLogRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isFailoverLog() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) mutationTopicRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isMutationTopic() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) restartVbucketsRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isRestartVbuckets() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) shutdownVbucketsRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isShutdownVbuckets() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) addBucketsRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isAddBuckets() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) delBucketsRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isDelBuckets() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) addInstancesRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isAddInstances() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) delInstancesRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isDelInstances() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) repairEndpointsRequests() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isRepairEndpoints() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

func (rs Requests) shutdownTopics() Requests {
	reqs := make(Requests, 0)
	for _, r := range rs {
		if r.reqmsg.isShutdownTopic() {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

//------------------------
// Validation on log lines.
//------------------------

func validate(msgs []*LogMsg) {
	if !validatePRAMRegistration(msgs) {
		log.Fatalf("validatePRAMRegistration\n")
	}
}

func validatePRAMRegistration(msgs []*LogMsg) bool {
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
	for _, msg := range msgs {
		if msg.hasPattern(re) {
			count++
		}
	}
	if count != 14 {
		log.Printf("validaterPRAMRegistration(): %v", count)
		return false
	}
	return true
}

//----------------
// local functions
//----------------

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
