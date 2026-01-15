package common

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/gocbcrypto"
	"github.com/couchbase/indexing/secondary/memdb"
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

func GetMemDBSnapshots(slicePath string, retry bool, getKeyById memdb.GetKeyByIdCb) ([]*MemdbSnapshotInfo, error) {
	var files []string
	pattern := "*/manifest.json"

	var infos []*MemdbSnapshotInfo

	for i := 0; i < 100; i++ {
		all, err := filepath.Glob(filepath.Join(slicePath, pattern))
		if err != nil {
			log.Printf("Error in filepath.Glob %v\n", err)
			continue
		}

		for _, f := range all {
			if !strings.Contains(f, tmpDirName) {
				files = append(files, f)
			}
		}
		sort.Strings(files)

		for i := len(files) - 1; i >= 0; i-- {
			f := files[i]
			info := &MemdbSnapshotInfo{DataPath: filepath.Dir(f)}
			fd, err := os.Open(f)
			if err == nil {
				defer fd.Close()
				bs, err := io.ReadAll(fd)
				if err == nil {
					if gocbcrypto.IsBytesEncrypted(bs) {
						if getKeyById != nil {
							fn := func([]byte) []byte {
								key, _, _ := getKeyById(nil)
								return key
							}
							bs, err = gocbcrypto.ReadFile(f, fn, nil)
						} else {
							err = fmt.Errorf("nil GetKeyById callback")
						}
					}
				}

				if err == nil {
					err = json.Unmarshal(bs, info)
					if err == nil {
						infos = append(infos, info)
					} else {
						log.Printf("GetMemDBSnapshots error:%v", err)
					}
				}
			}
		}

		if len(infos) != 0 || !retry {
			break
		}

		time.Sleep(100 * time.Millisecond)
		log.Printf("GetMemDBSnapshots: retrying %v\n", i+1)
	}

	return infos, nil
}
