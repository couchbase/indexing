package randdocs

import (
	"fmt"
	"math"
	rnd "math/rand"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kshard/fvecs"
)

type SiftData struct {
	sync.Mutex
	vecCount int // Vector Count
	overflow int // Overflow of Vectors

	fd       *os.File
	dec      *fvecs.Decoder[float32]
	filename string

	truthFd    *os.File
	truthDec   *fvecs.Decoder[uint32]
	truthFName string
}

func OpenSiftData(filename string) *SiftData {
	r, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error while opening file: ", err)
	}

	d := fvecs.NewDecoder[float32](r)

	return &SiftData{fd: r, dec: d, filename: filename}
}

func (sd *SiftData) GetValue() (docid string, vecNum int, overflow int,
	value []float32, err error) {

	sd.Lock()
	defer sd.Unlock()

getdata:
	errCount := 0
	data, err := sd.dec.Read()
	if err != nil {
		if errCount > 1 {
			return "", 0, 0, nil, err
		}
		errCount++
		sd.reset()
		goto getdata
	}

	c, o := sd.vecCount, sd.overflow
	sd.vecCount = sd.vecCount + 1
	docid = fmt.Sprintf("%v_%v", o, c)

	return docid, c, o, data, nil
}

func (sd *SiftData) reset() error {
	sd.fd.Close()
	sd.fd = nil
	sd.dec = nil
	sd.vecCount = 0

	var err error
	sd.fd, err = os.Open(sd.filename)
	if err != nil {
		fmt.Println("Error while opening file: ", sd.filename, err)
		return err
	}

	sd.dec = fvecs.NewDecoder[float32](sd.fd)

	if sd.truthDec != nil {
		sd.truthFd.Close()
		sd.truthFd = nil
		sd.truthDec = nil

		var err error
		sd.truthFd, err = os.Open(sd.truthFName)
		if err != nil {
			fmt.Println("Error while opening file: ", sd.truthFName, err)
			return err
		}

		sd.truthDec = fvecs.NewDecoder[uint32](sd.truthFd)
	}

	sd.overflow++
	return nil
}

func OpenSiftQueryAndGroundTruth(queryFVecs, truthIVecs string) (*SiftData, error) {
	fd1, err := os.Open(queryFVecs)
	if err != nil {
		fmt.Println("Error while opening file: ", err)
		return nil, err
	}

	fd2, err := os.Open(truthIVecs)
	if err != nil {
		fmt.Println("Error while opening file: ", err)
		return nil, err
	}

	d1 := fvecs.NewDecoder[float32](fd1)
	d2 := fvecs.NewDecoder[uint32](fd2)

	return &SiftData{fd: fd1, dec: d1, filename: queryFVecs,
		truthFd: fd2, truthDec: d2, truthFName: truthIVecs}, nil
}

func (sd *SiftData) GetQueryAndTruth() (query []float32, nearestVecOffsets []uint32, err error) {
	sd.Lock()
	defer sd.Unlock()

	query, err = sd.dec.Read()
	if err != nil {
		return nil, nil, err
	}

	nearestVecOffsets, err = sd.truthDec.Read()
	if err != nil {
		return nil, nil, err
	}

	return query, nearestVecOffsets, nil
}

var otherStringData map[string][]string = map[string][]string{
	"type":     []string{"Casual", "Formal", "Both", "None"},
	"category": []string{"Shoes", "Trousers", "Shirts", "Socks"},
	"country":  []string{"USA", "AUS", "IND", "CAN"},
	"brand":    []string{"Nike", "Adidas", "Reebok", "Puma"},
	"color":    []string{"Green", "Red", "Blue", "Black"},
}

var otherIntData map[string][]int = map[string][]int{
	"size": []int{5},
}

func getSiftData(cfg Config, sd *SiftData, cnt *int64) (string, map[string]interface{}, error) {
	randgen := rnd.New(rnd.NewSource(int64(cfg.VecSeed)))
	value := make(map[string]interface{})

	docid, vecnum, overflow, sift, err := sd.GetValue()
	if err != nil {
		return "", nil, err
	}
	value["docid"] = docid
	value["vectornum"] = vecnum
	value["overflow"] = overflow
	value["sift"] = sift
	value["count"] = atomic.LoadInt64(cnt)

	value["gender"] = "male"
	if overflow%2 == 0 {
		value["gender"] = "female"
	}

	if overflow%3 == 0 {
		value["floats"] = math.Pi
	} else if overflow%3 == 1 {
		value["floats"] = math.E
	} else {
		value["floats"] = math.Phi
	}

	if overflow%4 == 0 {
		value["direction"] = "east"
	} else if overflow%4 == 1 {
		value["direction"] = "west"
	} else if overflow%4 == 2 {
		value["direction"] = "north"
	} else {
		value["direction"] = "south"
	}

	if overflow%10 != 0 {
		value["missing"] = "NotMissing"
	}

	for strKey, strValList := range otherStringData {
		value[strKey] = strValList[overflow%len(strValList)]
	}

	for strKey, intValList := range otherIntData {
		value[strKey] = intValList[overflow%len(intValList)]
	}

	value["phone"] = (10000000000 * (overflow % 10)) + randgen.Intn(100000000)
	value["docnum"] = overflow*10000 + vecnum

	return docid, value, nil
}
