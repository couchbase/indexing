package randdocs

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/json"
	"fmt"
	rnd "math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
)

type Config struct {
	ClusterAddr string
	Bucket      string
	Scope       string
	Collection  string

	NumDocs       int
	DocIdLen      int
	FieldSize     int
	ArrayLen      int
	JunkFieldSize int
	Iterations    int
	Threads       int
	DocNumOffset  int
	OpsPerSec     int

	// Vectors
	GenVectors     bool
	VecDimension   int
	UseSIFTSmall   bool
	SIFTFVecsFile  string
	SkipNormalData bool

	// Sparse Vectors
	GenSparseVectors bool
	SparseVecDim     int
	UseSparseSmall   bool
	SparseCSRFile    string

	// Seed that should be used to generate random vectors
	// Default is 1234
	VecSeed int

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
		fmt.Printf("Error observed when connecting to bucket: %v, err: %v\n", cfg.Bucket, err)
		return err
	}
	defer b.Close()

	var cnt int64
	fullStart := time.Now()

	opsPerSec := time.Duration(cfg.OpsPerSec / cfg.Threads)
	sleepPerOp := time.Second / opsPerSec
	fmt.Printf("randdocs: Sleep per op = [%v]\n", sleepPerOp)

	var sd *SiftData
	if cfg.UseSIFTSmall {
		sd = OpenSiftData(cfg.SIFTFVecsFile)
	}

	var sparseData *SparseData
	if cfg.UseSparseSmall {
		var sparseErr error
		sparseData, sparseErr = OpenSparseData(cfg.SparseCSRFile)
		if sparseErr != nil {
			fmt.Printf("Error opening sparse data file: %v\n", sparseErr)
			return sparseErr
		}
	}

	var cid string
	if cfg.Scope != "" && cfg.Collection != "" {
		cid, err = common.GetCollectionID(cfg.ClusterAddr, cfg.Bucket, cfg.Scope, cfg.Collection)
		if err != nil {
			fmt.Printf("Error observed when fetching collectionID err: %v\n", err)
			return err
		}

	}

	var localErr error
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

					var value map[string]interface{}
					if !cfg.SkipNormalData {
						value = generateJson()
						prefix := docid[cfg.DocIdLen-PREFIX_LEN : cfg.DocIdLen]
						suffix := randFromAlphabet(cfg.FieldSize, docid)
						value["body"] = fmt.Sprintf("%s-%s", prefix, suffix)
					}

					if cfg.UseSIFTSmall {
						docid, value, err = getSiftData(cfg, sd, &cnt)
						if err != nil {
							return
						}
					}

					if cfg.UseSparseSmall {
						docid, value, err = getSparseData(cfg, sparseData, &cnt)
						if err != nil {
							return
						}
					}

					if cfg.GenVectors {
						value["description"] = generateVectors(cfg.VecDimension, cfg.VecSeed)
					}

					if cfg.GenSparseVectors {
						key := "sparse_random"
						if cfg.SparseVecDim > 0 {
							key = "sparse_dim"
						}
						value[key] = generateSparseVector(cfg.SparseVecDim, cfg.VecSeed)
					}

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

					if cid != "" {
						localErr = b.SetC(docid, cid, 0, value)
					} else {
						localErr = b.Set(docid, 0, value)
					}
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
