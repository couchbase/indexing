package randdocs

import "crypto/rand"
import rnd "math/rand"
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
}

func randString(n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func Run(cfg Config) error {
	runtime.GOMAXPROCS(cfg.Threads)

	b, err := common.ConnectBucket(cfg.ClusterAddr, "default", cfg.Bucket)
	if err != nil {
		return err
	}
	defer b.Close()

	for itr := 0; itr < cfg.Iterations; itr++ {
		var wg sync.WaitGroup
		for thr := 0; thr < cfg.Threads; thr++ {
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				for i := 0; i < cfg.NumDocs/cfg.Threads; i++ {

					docid := fmt.Sprintf("doc-%0*d", cfg.DocIdLen, i+offset+cfg.DocNumOffset)
					value := make(map[string]interface{})
					value["field"] = randString(cfg.FieldSize)
					if cfg.JunkFieldSize != 0 {
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
				}
			}(thr * cfg.NumDocs / cfg.Threads)
		}
		wg.Wait()
	}

	return err
}
