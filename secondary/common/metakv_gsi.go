// @author Couchbase <info@couchbase.com>
// @copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package common

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	METAKV_SIZE_LIMIT = 2048
)

func MetakvGet(path string, v interface{}) (bool, error) {
	raw, _, err := metakv.Get(path)
	if err != nil {
		logging.Fatalf("MetakvGet: Failed to fetch %s from metakv: %s", path, err.Error())
	}

	if raw == nil {
		return false, err
	}

	err = json.Unmarshal(raw, v)
	if err != nil {
		logging.Fatalf("MetakvGet: Failed unmarshalling value for %s: %s\n%s",
			path, err.Error(), string(raw))
		return false, err
	}

	return true, nil
}

func MetakvSet(path string, v interface{}) error {
	raw, err := json.Marshal(v)
	if err != nil {
		logging.Fatalf("MetakvSet: Failed to marshal value for %s: %s\n%v",
			path, err.Error(), v)
		return err
	}

	err = metakv.Set(path, raw, nil)
	if err != nil {
		logging.Fatalf("MetakvSet Failed to set %s: %s", path, err.Error())
	}
	return err
}

func MetakvDel(path string) error {

	err := metakv.Delete(path, nil)
	if err != nil {
		logging.Fatalf("MetakvDel: Failed to delete %s: %s", path, err.Error())
	}
	return err
}

func MetakvRecurciveDel(dirpath string) error {

	err := metakv.RecursiveDelete(dirpath)
	if err != nil {
		logging.Errorf("MetakvRecurciveDel: Failed to delete %s: %s", dirpath, err.Error())
	}
	return err
}

// MetakvList returns a slice of all entries (tokens) stored in dirpath in metakv.
// The key of each token is in entry.Path and the token itself is the byte slice
// entry.Value which can be unmarshalled into a token of the appropriate type.
func MetakvList(dirpath string) ([]metakv.KVEntry, error) {
	if len(dirpath) == 0 {
		return nil, fmt.Errorf("Empty metakv path")
	}
	if string(dirpath[len(dirpath)-1]) != "/" {
		dirpath = dirpath + "/"
	}
	return metakv.ListAllChildren(dirpath)
}

// MetakvBigValueSet stores large tokens by breaking them up into pieces of max size
// METAKV_SIZE_LIMIT and storing the pieces under keys path/0, path/1, ....
func MetakvBigValueSet(path string, v interface{}) error {

	if len(path) == 0 {
		return fmt.Errorf("Empty metakv path")
	}

	if string(path[len(path)-1]) == "/" {
		path = path[:len(path)-1]
	}

	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}

	len := len(buf)
	count := 0

	for len > 0 {
		path2 := fmt.Sprintf("%v/%v", path, count)

		size := len
		if size > METAKV_SIZE_LIMIT {
			size = METAKV_SIZE_LIMIT
		}

		if err = metakv.Set(path2, buf[:size], nil); err != nil {
			MetakvBigValueDel(path)
			return err
		}

		count++
		len -= size
		if len > 0 {
			buf = buf[size:]
		}
	}

	return nil
}

func MetakvBigValueDel(path string) error {

	if len(path) == 0 {
		return fmt.Errorf("Empty metakv path")
	}

	if string(path[len(path)-1]) != "/" {
		path = fmt.Sprintf("%v/", path)
	}

	return MetakvRecurciveDel(path)
}

// MetakvBigValueGet takes the virtual path of a big value token and
// retrieves and reassembles all its metakv pieces (stored as values of
// children /0, /1, /2, ... of the virtual path) into the original token.
func MetakvBigValueGet(path string, value interface{}) (bool, error) {

	if len(path) == 0 {
		return false, fmt.Errorf("Empty metakv path")
	}

	if string(path[len(path)-1]) != "/" {
		path = fmt.Sprintf("%v/", path)
	}

	entries, err := metakv.ListAllChildren(path)
	if err != nil {
		return false, err
	}

	if len(entries) == 0 {
		// value not exist
		return false, nil
	}

	raws := make([][]byte, len(entries))
	for _, entry := range entries {
		loc := strings.LastIndex(entry.Path, "/")
		if loc == -1 || loc == len(entry.Path)-1 {
			return false, fmt.Errorf("Unable to identify index for %v", entry.Path)
		}
		index, err := strconv.Atoi(entry.Path[loc+1:])
		if err != nil {
			return false, err
		}
		if index >= len(entries) {
			//The value is not fully formed.
			// Do not return error, but says value not exist.
			return false, nil
		}
		raws[index] = entry.Value
	}

	var buf []byte
	for _, raw := range raws {
		//The value is not fully formed.
		// Do not return error, but says value not exist.
		if raw == nil {
			return false, nil
		}
		buf = append(buf, raw...)
	}

	if err := json.Unmarshal(buf, value); err != nil {
		return false, err
	}

	return true, nil
}

// MetakvBigValueList lists the virtual paths of each big value token stored
// under dirpath. There are no metakv values associated with these virtual
// paths as each big value token is divided into multiple metakv values
// as children /0, /1, /2, ... of its virtual path. The caller must do a
// separate MetakvBigValueGet for each virtual path returned to retrieve and
// reassemble all the child pieces of the associated big value token.
func MetakvBigValueList(dirpath string) ([]string, error) {

	if len(dirpath) == 0 {
		return nil, fmt.Errorf("Empty metakv path")
	}

	if string(dirpath[len(dirpath)-1]) != "/" {
		dirpath = fmt.Sprintf("%v/", dirpath)
	}

	entries, err := metakv.ListAllChildren(dirpath)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	paths := make(map[string]bool)
	for _, entry := range entries {
		path := entry.Path

		loc := strings.LastIndex(path, "/")
		if loc == -1 || loc == len(path)-1 {
			continue
		}
		path = path[:loc]

		if len(path) <= len(dirpath) {
			continue
		}

		paths[path] = true
	}

	var result []string
	for path, _ := range paths {
		result = append(result, path)
	}

	return result, nil
}
