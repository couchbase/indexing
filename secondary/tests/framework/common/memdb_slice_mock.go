package common

import (
	"container/list"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const tmpDirName = ".tmp" // Same as indexer.tmpDirName

type MemdbSnapshotInfo struct {
	DataPath string
}

type snapshotInfoContainer struct {
	snapshotList *list.List
}

func (sc *snapshotInfoContainer) GetLatest() *MemdbSnapshotInfo {
	e := sc.snapshotList.Front()

	if e == nil {
		return nil
	} else {
		x := e.Value.(MemdbSnapshotInfo)
		return &x
	}
}

func (sc *snapshotInfoContainer) List() []MemdbSnapshotInfo {
	var infos []MemdbSnapshotInfo
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		info := e.Value.(MemdbSnapshotInfo)
		infos = append(infos, info)
	}
	return infos
}

func NewSnapshotInfoContainer(infos []*MemdbSnapshotInfo) *snapshotInfoContainer {
	sc := &snapshotInfoContainer{snapshotList: list.New()}

	for _, info := range infos {
		sc.snapshotList.PushBack(*info)
	}

	return sc
}

func GetMemDBSnapshots(slicePath string) ([]*MemdbSnapshotInfo, error) {
	var files []string
	pattern := "*/manifest.json"
	all, _ := filepath.Glob(filepath.Join(slicePath, pattern))
	for _, f := range all {
		if !strings.Contains(f, tmpDirName) {
			files = append(files, f)
		}
	}
	sort.Strings(files)

	var infos []*MemdbSnapshotInfo
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		info := &MemdbSnapshotInfo{DataPath: filepath.Dir(f)}
		fd, err := os.Open(f)
		if err == nil {
			defer fd.Close()
			bs, err := ioutil.ReadAll(fd)
			if err == nil {
				err = json.Unmarshal(bs, info)
				if err == nil {
					infos = append(infos, info)
				}
			}
		}
	}
	return infos, nil
}
