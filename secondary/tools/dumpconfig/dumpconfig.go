package main

import "flag"
import "io/ioutil"
import "log"
import "os"
import "fmt"
import "sort"
import "strings"

import c "github.com/couchbase/indexing/secondary/common"

var options struct {
	outfile string
	format  string
}

func argParse() {
	flag.StringVar(&options.format, "format", "rst",
		"format of configuration option, default is .md")
	flag.StringVar(&options.outfile, "out", "",
		"specify file to dump the configuration options")

	flag.Parse()

	if options.format != "rst" || options.outfile == "" {
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	outtxt := "2i configuration parameters\n"
	outtxt += "===========================\n"
	argParse()
	for _, param := range sortParams() {
		cv := c.SystemConfig[param]
		outtxt += "\n"
		outtxt += fmt.Sprintf("**%s** (%T)\n", param, cv.DefaultVal)
		outtxt += fmt.Sprintf("    %s\n", cv.Help)
	}
	err := ioutil.WriteFile(options.outfile, []byte(outtxt), 0660)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Written %v bytes to %v\n", len(outtxt), options.outfile)
}

func sortParams() []string {
	m := make(map[string]interface{})
	for param := range c.SystemConfig {
		m = populateMap(m, strings.Split(param, "."))
	}
	params := []string{}
	for _, param := range paramList(m, "") {
		params = append(params, strings.Trim(param, "."))
	}
	return params
}

func populateMap(m map[string]interface{}, keys []string) map[string]interface{} {
	head, rest := keys[0], keys[1:]
	if len(rest) == 0 {
		m[head] = nil
		return m
	}
	if _, ok := m[head]; !ok {
		m[head] = make(map[string]interface{})
	}
	m[head] = populateMap(m[head].(map[string]interface{}), rest)
	return m
}

func paramList(m map[string]interface{}, prefix string) []string {
	params := []string{}
	for _, key := range sortMap(m) {
		newp := prefix + "." + key
		if m[key] == nil {
			params = append(params, newp)
		} else {
			params = append(
				params, paramList(m[key].(map[string]interface{}), newp)...)
		}
	}
	return params
}

func sortMap(m map[string]interface{}) []string {
	params := []string{}
	keys := []string{}
	for k, v := range m {
		if v == nil {
			params = append(params, k)
		} else {
			keys = append(keys, k)
		}
	}
	sort.Strings(params)
	sort.Strings(keys)
	params = append(params, keys...)
	return params
}
