//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import "bytes"
import "flag"
import "fmt"
import "io/ioutil"
import "log"
import "os"
import "path"
import "sort"
import "strings"

import "github.com/prataprc/collatejson"

type codeObj struct {
	off  int
	code []byte
}
type codeList []codeObj

var options struct {
	arrLenPrefix bool
	mapLenPrefix bool
}

var usageHelp = `Usage : checkfiles [OPTIONS] <file> | <dir>
specifying a file <file> will sort each line in the file,
    assuming each line as valid json.
specifying a dir <dir> will pick each file in dir and sort each
    line in the file, if corresponding <file>.ref is found inside
    the same dir, the output will compared with <file>.ref file.

`

func argParse() string {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usageHelp)
		flag.PrintDefaults()
	}
	flag.BoolVar(&options.arrLenPrefix, "arrlen", false,
		"sort array by length")
	flag.BoolVar(&options.mapLenPrefix, "maplen", true,
		"sort json object by length")
	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
		os.Exit(64)
	}

	return args[0]
}

func main() {
	file := argParse()
	if fi, err := os.Stat(file); err != nil {
		panic(fmt.Errorf("error stating %v", file))
	} else if fi.IsDir() {
		runTests(file)
	} else {
		fmt.Println(strings.Join(sortFile(file), "\n"))
	}
}

func runTests(rootdir string) {
	if entries, err := ioutil.ReadDir(rootdir); err == nil {
		for _, entry := range entries {
			file := path.Join(rootdir, entry.Name())
			if !strings.HasSuffix(file, ".ref") {
				log.Println("Checking", file, "...")
				out := strings.Join(sortFile(file), "\n")
				if ref, err := ioutil.ReadFile(file + ".ref"); err != nil {
					panic(fmt.Errorf("error reading reference file %v", file))
				} else if strings.Trim(string(ref), "\n") != out {
					fmt.Println(out)
					panic(fmt.Errorf("sort mismatch in %v", file))
				}
			}
		}
	} else {
		panic(err)
	}
}

func (codes codeList) Len() int {
	return len(codes)
}

func (codes codeList) Less(i, j int) bool {
	return bytes.Compare(codes[i].code, codes[j].code) < 0
}

func (codes codeList) Swap(i, j int) {
	codes[i], codes[j] = codes[j], codes[i]
}

func sortFile(filename string) (outs []string) {
	s, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err.Error())
	}
	codec := collatejson.NewCodec(100)
	codec.SortbyArrayLen(options.arrLenPrefix)
	codec.SortbyPropertyLen(options.mapLenPrefix)
	outs = encodeLines(codec, s)
	return
}

func encodeLines(codec *collatejson.Codec, s []byte) []string {
	var err error
	texts, codes := lines(s), make(codeList, 0)
	for i, x := range texts {
		code := make([]byte, 0, len(x)*3+collatejson.MinBufferSize)
		if code, err = codec.Encode(x, code); err != nil {
			log.Fatal(err)
		}
		codes = append(codes, codeObj{i, code})
	}
	outs := doSort(texts, codes)
	return outs
}

func doSort(texts [][]byte, codes codeList) (outs []string) {
	sort.Sort(codes)
	for _, code := range codes {
		outs = append(outs, string(texts[code.off]))
	}
	return
}

func lines(content []byte) [][]byte {
	content = bytes.Trim(content, "\r\n")
	return bytes.Split(content, []byte("\n"))
}
