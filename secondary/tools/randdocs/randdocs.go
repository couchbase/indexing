package randdocs

import (
	"crypto/rand"
	"encoding/json"
	rnd "math/rand"
	"sync/atomic"
	"time"
)
import "crypto/md5"
import "fmt"
import "sync"
import "runtime"
import "github.com/couchbase/indexing/secondary/common"

type Config struct {
	ClusterAddr   string
	Bucket        string
	NumDocs       int
	DocIdLen      int
	FieldSize     int
	ArrayLen      int
	JunkFieldSize int
	Iterations    int
	Threads       int
	DocNumOffset  int
	OpsPerSec     int

	// Use 16 byte random docid
	UseRandDocID bool
}

const (
	PREFIX_LEN = 12
	ALPHANUM   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

func randFromAlphabet(n int, alphabet string) string {
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphabet[b%byte(len(alphabet))]
	}
	return string(bytes)
}

func randString(n int) string {
	return randFromAlphabet(n, ALPHANUM)
}

func Run(cfg Config) error {
	cfgBytes, err := json.MarshalIndent(cfg, "", "    ")
	if err != nil {
		return err
	}
	fmt.Printf("randdocs: Runing with cfg: \n%s\n", string(cfgBytes))

	runtime.GOMAXPROCS(cfg.Threads)

	b, err := common.ConnectBucket(cfg.ClusterAddr, "default", cfg.Bucket)
	if err != nil {
		return err
	}
	defer b.Close()

	var cnt int64
	fullStart := time.Now()

	opsPerSec := time.Duration(cfg.OpsPerSec / cfg.Threads)
	sleepPerOp := time.Second / opsPerSec
	fmt.Printf("randdocs: Sleep per op = [%v]\n", sleepPerOp)

	for itr := 0; itr < cfg.Iterations; itr++ {
		var wg sync.WaitGroup
		for thr := 0; thr < cfg.Threads; thr++ {
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				for i := 0; i < cfg.NumDocs/cfg.Threads; i++ {
					start := time.Now()
					docid := fmt.Sprintf("%0*d", cfg.DocIdLen, i+offset+cfg.DocNumOffset)[:cfg.DocIdLen]

					if cfg.UseRandDocID {
						key := md5.Sum([]byte(docid))
						docid = fmt.Sprintf("%x", key)[:cfg.DocIdLen]
					}

					prefix := docid[cfg.DocIdLen-PREFIX_LEN : cfg.DocIdLen]
					suffix := randFromAlphabet(cfg.FieldSize, docid)

					value := make(map[string]interface{})
					value["body"] = fmt.Sprintf("%s-%s", prefix, suffix)

					if cfg.JunkFieldSize != 0 {
						value["field"] = randString(cfg.FieldSize)
						value["junk"] = fmt.Sprintf("%0*d", cfg.JunkFieldSize, 0)
					}

					if cfg.ArrayLen > 0 {
						seed := rnd.Int() % 1000000
						val := []int{}
						for i := 0; i < cfg.ArrayLen; i++ {
							val = append(val, seed+i)
						}
						value["arr"] = val
					}

					localErr := b.Set(docid, 0, value)
					if localErr != nil {
						fmt.Println(err)
						err = localErr
					}

					if k := atomic.AddInt64(&cnt, 1); k%100000 == 0 {
						fmt.Printf("Set %7d docs at %dops/sec\n", k, k/(1+int64(time.Since(fullStart).Seconds())))
					}

					dur := time.Since(start)
					toSleep := sleepPerOp - dur
					if toSleep > 0 {
						time.Sleep(toSleep)
					}
				}
			}(thr * cfg.NumDocs / cfg.Threads)
		}
		wg.Wait()

		fmt.Printf("Done setting [%d] docs at [%v]sets/sec and itr [%d]\n", cnt, cnt/(1+int64(time.Since(fullStart).Seconds())), itr)
	}

	return err
}
