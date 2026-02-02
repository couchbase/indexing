//go:build nolint

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

//import "encoding/json"

type filename string
type testname string
type statname string
type statval float64

type aggstats struct {
	data map[filename]*filestat
}

type filestat struct {
	current testname
	data    map[testname]map[statname]statval
}

type aggmtx struct {
	data map[testname]map[filename]map[statname]statval
}

func html(mtx *aggmtx, agg *aggstats) string {
	_, tests, _ := agg.dimensions()
	buf := NewBytesBuffer()
	buf.Printf("%v", `<html>
  <head>
    <script type="text/javascript"
          src="https://www.google.com/jsapi?autoload={
            'modules':[{
              'name':'visualization',
              'version':'1',
              'packages':['corechart']
            }]
          }">
    </script>
    <script src='nprogress/nprogress.js'></script>
    <link rel='stylesheet' href='nprogress/nprogress.css'/>
    <script type="text/javascript">
      google.setOnLoadCallback(loaded);
      function loaded() {
        NProgress.start()
        window.setTimeout(drawChart, 500);
      }
`)
	buf.Printf("%v", js(mtx, agg))
	buf.Printf("%v", `</script>
  </head>
  <body>
`)
	buf.Printf("   <small><ul style='list-style-type:circle'>\n")
	for _, tn := range *tests {
		buf.Printf("   <li><a href='#%v'>%v</a></li>\n", tn, prettyName(tn))
	}
	buf.Printf("   </ul></small>\n")
	for ti, tn := range *tests {
		buf.Printf("   <a name='%v'></a>\n", tn)
		buf.Printf("   <div id='curve_chart_%v' style='width: 900px; height: 500px'></div>\n", ti)
	}
	buf.Printf("%v", `  </body>
</html>
`)
	return buf.String(0)
}

func js(mtx *aggmtx, agg *aggstats) string {
	files, tests, stats := agg.dimensions()
	lint := len(*files) / 5
	buf := NewBytesBuffer()
	buf.Printf("\nvar label2file = {};\n")
	buf.Printf("function drawChart() {\n")
	for ti, test := range *tests {
		lbl := fmt.Sprintf("%v", ti)
		buf.Printf("\nvar data_%v = new google.visualization.DataTable();\n", lbl)
		buf.Printf("data_%v.addColumn('string', 'Run');\n", lbl)
		for _, stat := range *stats {
			buf.Printf("data_%v.addColumn('number', '%s');\n", lbl, string(stat))
		}
		for fi, fn := range *files {
			fnp := strings.Split(string(fn), "/")
			buf.Printf("data_%v.addRow();\n", lbl)
			buf.Printf("data_%v.setCell(%v, 0, '%v');\n", lbl, fi, prettyLabel(fn))
			buf.Printf("label2file['%v'] = '%v';\n", prettyLabel(fn), fnp[len(fnp)-1])
			for si, stat := range *stats {
				val := float64(mtx.data[test][fn][stat])
				if val == 0 {
					continue
				}
				buf.Printf("data_%v.setCell(%v, %v, %f);\n", lbl, fi, si+1, val)
			}
		}

		buf.Printf("var options_%v = {title:'%v', curveType:'none', legend:{position:'right'}, hAxis:{showTextEvery:%v}};\n", lbl, prettyName(test), lint)
		buf.Printf("var chart_%v = new google.visualization.LineChart(document.getElementById('curve_chart_%v'));\n", lbl, lbl)
		buf.Printf("chart_%v.draw(data_%v, options_%v);\n", lbl, lbl, lbl)
		buf.Printf("google.visualization.events.addListener(chart_%v, 'select', function() {\n", lbl)
		buf.Printf(" var selection = chart_%v.getSelection();\n", lbl)
		buf.Printf(" var row = selection[0].row;\n")
		buf.Printf(" var label = data_%v.getValue(row, 0);\n", lbl)
		buf.Printf(" var fname = label2file[label];\n")
		buf.Printf(" document.location.href = fname;\n")
		buf.Printf("});\n")
		buf.Printf("NProgress.set(%f);\n", float64(ti)/float64(len(*tests))+0.2)
	}
	buf.Printf("NProgress.done();\n")
	buf.Printf("}\n")
	return buf.String(6)
}

func transform(agg *aggstats) *aggmtx {
	files, tests, stats := agg.dimensions()
	mtx := aggmtx{data: make(map[testname]map[filename]map[statname]statval, len(*tests))}
	for _, test := range *tests {
		mtx.data[test] = make(map[filename]map[statname]statval, len(*files))
		for _, file := range *files {
			mtx.data[test][file] = make(map[statname]statval, len(*stats))
		}
	}

	for fn := range agg.data {
		fstat := agg.data[fn]
		for test := range fstat.data {
			statkv := fstat.data[test]
			for statk := range statkv {
				statv := statkv[statk]
				mtx.data[test][fn][statk] = statv
			}
		}

	}

	return &mtx
}

func (agg *aggstats) dimensions() (*[]filename, *[]testname, *[]statname) {
	mfiles := make(map[filename]bool, 1024)
	mtests := make(map[testname]bool, 512)
	mstats := make(map[statname]bool, 16)

	for fname := range agg.data {
		mfiles[fname] = true
		fstat := agg.data[fname]
		for test := range fstat.data {
			mtests[test] = true
			statkv := fstat.data[test]
			for statk := range statkv {
				mstats[statk] = true
			}
		}
	}
	afnames := make([]filename, 0, len(mfiles))
	for fn := range mfiles {
		afnames = append(afnames, fn)
	}
	atests := make([]testname, 0, len(mtests))
	for test := range mtests {
		atests = append(atests, test)
	}
	astats := make([]statname, 0, len(mstats))
	for stat := range mstats {
		astats = append(astats, stat)
	}
	sort.Sort(ByFn(afnames))
	sort.Sort(ByTest(atests))
	sort.Sort(ByStat(astats))
	return &afnames, &atests, &astats
}

type ByFn []filename
type ByTest []testname
type ByStat []statname

func (a ByFn) Len() int           { return len(a) }
func (a ByFn) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFn) Less(i, j int) bool { return timestamp(a[i]).Before(timestamp(a[j])) }

func (a ByTest) Len() int           { return len(a) }
func (a ByTest) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTest) Less(i, j int) bool { return a[i] < a[j] }

func (a ByStat) Len() int           { return len(a) }
func (a ByStat) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByStat) Less(i, j int) bool { return a[i] < a[j] }

func main() {
	count := len(os.Args) - 1
	if count < 1 {
		fmt.Println("Usage: perfstat <file> [<file>]...")
		return
	}

	agg := aggstats{data: make(map[filename]*filestat, count)}
	for _, sfn := range os.Args[1:] {
		fn := filename(sfn)
		result := processBuild(fn)
		agg.data[fn] = result
	}
	mtx := transform(&agg)
	htm := html(mtx, &agg)
	fmt.Println(htm)
}

func processBuild(fn filename) *filestat {
	result := filestat{current: "", data: make(map[testname]map[statname]statval, 512)}
	data, err := ioutil.ReadFile(string(fn))
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		result.processLine(line)
	}
	// fmt.Println("\n\nprocessBuild :: result is ", result)
	return &result
}

var rePerfStat = regexp.MustCompile(`.*PERFSTAT\s+([^\s]+)\s+([^\s]+)`)
var reTestName = regexp.MustCompile(`=== RUN\s+([^\s]+)`)

func (fs *filestat) processLine(line string) {
	if !strings.Contains(line, "=== RUN") && !strings.Contains(line, "PERFSTAT") {
		return
	}

	result := reTestName.FindStringSubmatch(line)
	if len(result) == 2 {
		fs.current = testname(result[1])
		return
	}

	if fs.current == "" {
		return
	}

	result = rePerfStat.FindStringSubmatch(line)
	if len(result) != 3 {
		return
	}
	measure, svalue := statname(result[1]), result[2]
	fvalue, err := strconv.ParseFloat(svalue, 64)
	if err != nil {
		fmt.Println("Skipping unexpected value for float on line:", line)
		return
	}
	value := statval(fvalue)

	ops, present := fs.data[fs.current]
	if !present {
		ops = make(map[statname]statval, 10)
		fs.data[fs.current] = ops
	}
	accu, present := ops[measure]
	if !present {
		accu = 0
	}
	accu += value
	ops[measure] = accu
}

type bytesBuffer struct {
	buf *bytes.Buffer
}

func NewBytesBuffer() *bytesBuffer {
	return &bytesBuffer{buf: &bytes.Buffer{}}
}

func (buf bytesBuffer) String(indent int) string {
	if indent < 1 {
		return buf.buf.String()
	}
	leader := strings.Repeat(" ", indent)
	fbuf := bytes.Replace(buf.buf.Bytes(), []byte("\n"), []byte("\n"+leader), -1)
	return string(fbuf)
}

func (buf bytesBuffer) Printf(spec string, args ...interface{}) {
	buf.buf.WriteString(fmt.Sprintf(spec, args...))
}

func prettyLabel(fn filename) string {
	// fmt.Println("\n\nfunc prettyLabel :: filename is ", fn)
	ts := timestamp(fn)
	label := ts.Format("Jan 2, 2006 3pm")
	return label
}

var reTimestamp = regexp.MustCompile(`.+?-(.+?)-(.+?)\.(?:htm|perf\.html)`)

func timestamp(fn filename) time.Time {
	ets := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	result := reTimestamp.FindStringSubmatch(string(fn))
	//fmt.Println("\n\ntimestamp :: result is ", result)
	if len(result) != 3 {
		fmt.Println("Could not parse timestamp:", string(fn))
		return ets
	}
	label := result[2]
	//fmt.Println("func timestamp:: label is ", label)
	ts, err := time.Parse("02.01.2006-15.04", label)
	if err != nil {
		fmt.Println("Could not parse label: %v, Error = %v", label, err)
		return ets
	}
	return ts
}

func prettyName(test testname) string {
	name := string(test)
	var pretty string
	spacing := false
	for i := 0; i < len(name); i++ {
		switch {
		case name[i] >= 'A' && name[i] <= 'Z':
			if !spacing {
				pretty += " "
				spacing = true
			}
			pretty += name[i : i+1]
		case name[i] == '_':
			pretty += ":"
			spacing = false
		default:
			pretty += name[i : i+1]
			spacing = false
		}
	}
	return pretty
}
