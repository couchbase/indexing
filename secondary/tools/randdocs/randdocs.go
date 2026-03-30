package randdocs

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/json"
	"fmt"
	rnd "math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/gocb/v2"
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

	// Vector in XATTR
	VectorInXATTR bool

	// Optional credentials for gocb XATTR insertion.
	// If empty, credentials are resolved from CBAUTH_REVRPC_URL or CB_USERNAME/CB_PASSWORD env vars.
	Username string
	Password string
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

	var gocbCol *gocb.Collection
	var gocbCluster *gocb.Cluster
	if cfg.VectorInXATTR {
		gocbCluster, gocbCol, err = openGocbCollection(cfg.ClusterAddr, cfg.Bucket, cfg.Scope, cfg.Collection, cfg.Username, cfg.Password)
		if err != nil {
			fmt.Printf("Error connecting gocb for XATTR: %v\n", err)
			return err
		}
		defer gocbCluster.Close(nil)
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

					if cfg.VectorInXATTR {
						// For XATTR mode, don't include vector in main document
						// Only include scalar field
						value["type"] = "Document" // default scalar field

						var vectorData interface{}

						if cfg.GenVectors {
							value["description"] = []interface{}{} // placeholder
							vectorData = generateVectors(cfg.VecDimension, cfg.VecSeed)
						}

						if cfg.GenSparseVectors {
							key := "sparse_random"
							if cfg.SparseVecDim > 0 {
								key = "sparse_dim"
							}
							value[key] = nil // placeholder
							vectorData = generateSparseVector(cfg.SparseVecDim, cfg.VecSeed)
						}

						if cfg.UseSIFTSmall {
							vectorData = value["sift"]
							delete(value, "sift")
						}

						if cfg.UseSparseSmall {
							vectorData = value["sparse"]
							delete(value, "sparse")
						}

						if vectorData != nil {
							// Insert doc with scalar fields first
							if cid != "" {
								localErr = b.SetC(docid, cid, 0, value)
							} else {
								localErr = b.Set(docid, 0, value)
							}

							if localErr != nil {
								fmt.Println(err)
								err = localErr
							}

							// Then insert vector as XATTR using gocb
							xattrErr := insertVectorAsXATTR(gocbCol, docid, vectorData)
							if xattrErr != nil {
								fmt.Printf("Failed to insert XATTR for doc %s: %v\n", docid, xattrErr)
							}
						} else {
							// No vector data, just insert the document
							if cid != "" {
								localErr = b.SetC(docid, cid, 0, value)
							} else {
								localErr = b.Set(docid, 0, value)
							}
							if localErr != nil {
								fmt.Println(err)
								err = localErr
							}
						}
					} else {
						if cid != "" {
							localErr = b.SetC(docid, cid, 0, value)
						} else {
							localErr = b.Set(docid, 0, value)
						}
						if localErr != nil {
							fmt.Println(err)
							err = localErr
						}
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

// openGocbCollection creates a single gocb cluster connection and returns the target collection.
// The caller is responsible for closing the cluster when done.
// getAddrWithMemcachedPort converts cluster address to memcached port.
// gocb/v2 returns CONNECTION_ERROR with http/https ports (9000/19000), so use memcached port 12000.
func getAddrWithMemcachedPort(addr string) (string, error) {
	parts := strings.Split(addr, ":")
	if len(parts) == 1 {
		return "", fmt.Errorf("invalid cluster address: %v", addr)
	}
	return parts[0] + ":12000", nil
}

// resolveGocbCredentials returns credentials for gocb by trying, in order:
//  1. cfg.Username / cfg.Password if set (programmatic use, e.g. tests)
//  2. CBAUTH_REVRPC_URL env var (e.g. "http://Administrator:asdasd@127.0.0.1:9000/query")
//  3. CB_USERNAME / CB_PASSWORD env vars
func resolveGocbCredentials(username, password string) (string, string, error) {
	if username != "" {
		return username, password, nil
	}
	if raw := os.Getenv("CBAUTH_REVRPC_URL"); raw != "" {
		if u, err := url.Parse(raw); err == nil && u.User != nil {
			user := u.User.Username()
			pass, _ := u.User.Password()
			if user != "" {
				return user, pass, nil
			}
		}
	}
	if u := os.Getenv("CB_USERNAME"); u != "" {
		return u, os.Getenv("CB_PASSWORD"), nil
	}
	return "", "", fmt.Errorf("no credentials: set CBAUTH_REVRPC_URL or CB_USERNAME/CB_PASSWORD env vars")
}

func openGocbCollection(clusterAddr, bucket, scope, collection, username, password string) (*gocb.Cluster, *gocb.Collection, error) {
	connString, err := getAddrWithMemcachedPort(clusterAddr)
	if err != nil {
		return nil, nil, err
	}

	username, password, err = resolveGocbCredentials(username, password)
	if err != nil {
		return nil, nil, err
	}

	cluster, err := gocb.Connect("couchbase://"+connString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, nil, err
	}

	bucketObj := cluster.Bucket(bucket)
	if err = bucketObj.WaitUntilReady(10*time.Second, nil); err != nil {
		cluster.Close(nil)
		return nil, nil, err
	}

	var col *gocb.Collection
	if scope != "" && collection != "" {
		col = bucketObj.Scope(scope).Collection(collection)
	} else {
		col = bucketObj.Scope("_default").Collection("_default")
	}

	return cluster, col, nil
}

// insertVectorAsXATTR inserts vector data as an XATTR on an existing document.
func insertVectorAsXATTR(col *gocb.Collection, docid string, vectorData interface{}) error {
	// Determine XATTR key based on vector type
	var xattrKey string
	switch v := vectorData.(type) {
	case []float32:
		xattrKey = "vector_xattr"
	case SparseVector:
		xattrKey = "sparse_xattr"
	case []interface{}:
		xattrKey = "sparse_xattr"
	default:
		return fmt.Errorf("unsupported vector type: %T", v)
	}

	_, err := col.MutateIn(docid, []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, vectorData, &gocb.UpsertSpecOptions{IsXattr: true}),
	}, nil)
	return err
}


