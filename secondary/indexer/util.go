//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	MAX_GETSEQS_RETRIES      = 10
	MAX_GETITEMCOUNT_RETRIES = 10
)

func IsIPLocal(ip string) bool {

	netIP := net.ParseIP(ip)

	//if loopback address, return true
	if netIP.IsLoopback() {
		return true
	}

	//compare with the local ip
	if localIP, err := GetLocalIP(); err == nil {
		if localIP.Equal(netIP) {
			return true
		}
	}

	return false

}

func GetLocalIP() (net.IP, error) {

	tt, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, t := range tt {
		aa, err := t.Addrs()
		if err != nil {
			return nil, err
		}
		for _, a := range aa {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			v4 := ipnet.IP.To4()
			if v4 == nil || v4[0] == 127 { // loopback address
				continue
			}
			return v4, nil
		}
	}
	return nil, errors.New("cannot find local IP address")
}

func IndexPath(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	return fmt.Sprintf("%s_%s_%d_%d.index", inst.Defn.Bucket, inst.Defn.Name, instId, partnId)
}

func CodebookPath(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	indexPath := IndexPath(inst, partnId, sliceId)
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	codebookName := fmt.Sprintf("%s_%s_%d_%d.codebook", inst.Defn.Bucket, inst.Defn.Name, instId, partnId)
	return filepath.Join(indexPath, CODEBOOK_DIR, codebookName)
}

func InitCodebookDir(
	storeEngineDir string, idxInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId,
) error {
	// Construct codebookDirPath path
	codebookDirPath := filepath.Join(
		storeEngineDir,
		IndexPath(idxInst, partnId, sliceId),
		CODEBOOK_DIR,
	)

	//  Check the presence of codebook dir. Create one if it does not exist
	if _, err := iowrap.Os_Stat(codebookDirPath); err != nil {
		if os.IsNotExist(err) {
			return iowrap.Os_MkdirAll(codebookDirPath, 0755)
		} else {
			logging.Errorf("InitCodebookDir: Error observed while checking the presence of "+
				"codebookDir: %v, err: %v", codebookDirPath, err)
			return err
		}
	}
	return nil
}

func RemoveCodebookDir(
	storeEngineDir string, idxInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId,
) error {

	codebookDirPath := filepath.Join(
		storeEngineDir,
		IndexPath(idxInst, partnId, sliceId),
		CODEBOOK_DIR,
	)

	return iowrap.Os_RemoveAll(codebookDirPath)
}

// This has to follow the pattern in IndexPath function defined above.
func GetIndexPathPattern() string {
	return "*_*_*_*.index"
}

// This has to follow the pattern in IndexPath function defined above.
func GetInstIdPartnIdFromPath(idxPath string) (common.IndexInstId,
	common.PartitionId, error) {

	idxPath = strings.TrimPrefix(idxPath, common.BHIVE_DIR_PREFIX)

	pathComponents := strings.Split(idxPath, "_")
	if len(pathComponents) < 4 {
		err := errors.New(fmt.Sprintf("Malformed index path %v", idxPath))
		return common.IndexInstId(0), common.PartitionId(0), err
	}

	strInstId := pathComponents[len(pathComponents)-2]
	instId, err := strconv.ParseUint(strInstId, 10, 64)
	if err != nil {
		return common.IndexInstId(0), common.PartitionId(0), err
	}

	partnComponents := strings.Split(pathComponents[len(pathComponents)-1], ".")
	if len(partnComponents) != 2 {
		err := errors.New(fmt.Sprintf("Malformed index path %v", idxPath))
		return common.IndexInstId(0), common.PartitionId(0), err
	}

	strPartnId := partnComponents[0]
	partnId, err := strconv.ParseUint(strPartnId, 10, 64)
	if err != nil {
		return common.IndexInstId(0), common.PartitionId(0), err
	}

	return common.IndexInstId(instId), common.PartitionId(partnId), nil
}

func GetRealIndexInstId(inst *common.IndexInst) common.IndexInstId {
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	return instId
}

func GetCollectionItemCount(cluster, pooln, keyspaceId, cid string) (uint64, error) {
	var itemCount uint64
	bucketn := GetBucketFromKeyspaceId(keyspaceId)

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("Indexer::GetCollectionItemCount error=%v Retrying (%d)", err, r)
		}

		itemCount, err = common.CollectionItemCount(cluster, pooln, bucketn, cid)

		return err
	}

	verbose := logging.IsEnabled(logging.Verbose)
	var start time.Time
	if verbose {
		start = time.Now()
	}
	rh := common.NewRetryHelper(MAX_GETITEMCOUNT_RETRIES, time.Millisecond, 1, fn)
	err := rh.Run()

	if err != nil {
		// then log an error and give-up
		fmsg := "Indexer::GetCollectionItemCount Error Connecting to KV Cluster %v"
		logging.Errorf(fmsg, err)
		return 0, err
	}

	if verbose {
		logging.Verbosef("Indexer::GetCollectionItemCount Time Taken %v", time.Since(start))
	}
	return itemCount, err
}

// GetCurrentKVTs gets the current KV timestamp vector for the specified number of vBuckets.
func GetCurrentKVTs(cluster, pooln, keyspaceId, cid string, numVBuckets int) (Timestamp, error) {

	var seqnos []uint64
	bucketn := GetBucketFromKeyspaceId(keyspaceId)

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("Indexer::getCurrentKVTs error=%v Retrying (%d)", err, r)
		}

		//if collection id has not been specified, use bucket level
		if cid == "" {
			seqnos, err = common.BucketSeqnos(cluster, pooln, bucketn)
		} else {
			seqnos, err = common.CollectionSeqnos(cluster, pooln, bucketn, cid)
		}

		return err
	}

	verbose := logging.IsEnabled(logging.Verbose)
	var start time.Time
	if verbose {
		start = time.Now()
	}
	rh := common.NewRetryHelper(MAX_GETSEQS_RETRIES, time.Millisecond, 1, fn)
	err := rh.Run()

	if err != nil {
		// then log an error and give-up
		fmsg := "Indexer::getCurrentKVTs Error Connecting to KV Cluster %v"
		logging.Errorf(fmsg, err)
		return nil, err
	}

	if len(seqnos) < numVBuckets {
		fmsg := "BucketSeqnos(): got ts only for %v vbs"
		return nil, fmt.Errorf(fmsg, len(seqnos))
	}

	ts := NewTimestamp(len(seqnos))
	for i := 0; i < len(seqnos); i++ {
		ts[i] = seqnos[i]
	}

	if verbose {
		logging.Verbosef("Indexer::getCurrentKVTs Time Taken %v", time.Since(start))
	}
	return ts, err
}

func ValidateBucket(cluster, bucket string, uuids []string) bool {

	var cinfo *common.ClusterInfoCache
	url, err := common.ClusterAuthUrl(cluster)
	if err == nil {
		cinfo, err = common.NewClusterInfoCache(url, DEFAULT_POOL)
	}
	if err != nil {
		logging.Fatalf("Indexer::Fail to init ClusterInfoCache : %v", err)
		common.CrashOnError(err)
	}

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		logging.Errorf("Indexer::Fail to init ClusterInfoCache : %v", err)
		common.CrashOnError(err)
	}

	if nids, err := cinfo.GetNodesByBucket(bucket); err == nil && len(nids) != 0 {
		// verify UUID
		currentUUID := cinfo.GetBucketUUID(bucket)
		for _, uuid := range uuids {
			if uuid != currentUUID {
				return false
			}
		}
		return true
	} else {
		logging.Fatalf("Indexer::Error Fetching Bucket Info: %v Nids: %v", err, nids)
		return false
	}

}

func IsEphemeral(cluster, bucket string) (bool, error) {
	var cinfo *common.ClusterInfoCache
	url, err := common.ClusterAuthUrl(cluster)
	if err == nil {
		cinfo, err = common.NewClusterInfoCache(url, DEFAULT_POOL)
	}
	if err != nil {
		logging.Fatalf("Indexer::Fail to init ClusterInfoCache : %v", err)
		common.CrashOnError(err)
	}
	cinfo.SetUserAgent("IsEphemeral")

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		logging.Errorf("Indexer::Fail to init ClusterInfoCache : %v", err)
		common.CrashOnError(err)
	}

	return cinfo.IsEphemeral(bucket)
}

// flip bits in-place for a given byte slice
func FlipBits(code []byte) {

	for i, b := range code {
		code[i] = ^b
	}
	return
}

func clusterVersion(clusterAddr string) uint64 {

	var cinfo *common.ClusterInfoCache
	url, err := common.ClusterAuthUrl(clusterAddr)
	if err != nil {
		return common.INDEXER_45_VERSION
	}

	cinfo, err = common.NewClusterInfoCache(url, DEFAULT_POOL)
	if err != nil {
		return common.INDEXER_45_VERSION
	}
	cinfo.SetUserAgent("clusterVersion")

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		return common.INDEXER_45_VERSION
	}

	return cinfo.GetClusterVersion()
}

// *******************************************
// Direct copy of util function from plasma
// *******************************************
func isNetPath(location string) bool {
	const urlSchemePattern = "^[a-zA-Z][a-zA-Z0-9+-.]*://"
	rex, _ := regexp.Compile(urlSchemePattern)
	return rex.MatchString(location)
}

// supports both file system paths and urls
// for net, parent url must contain a valid host/bucket
// do not use this function without a valid parent path
func joinURIPath(parent string, elem ...string) string {
	if isNetPath(parent) {
		urlPath := parent
		for _, e := range elem {
			urlPath = fmt.Sprintf("%s/%s", urlPath, filepath.ToSlash(e))
		}
		return urlPath
	} else {
		elem = append([]string{parent}, elem...)
		return filepath.Join(elem...)
	}
}

func Float32ToByteSlice(v []float32) []byte {
	var ft float32
	size := int(reflect.TypeOf(ft).Size())

	//TODO: Use SliceData
	var b []byte
	vsh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	bsh := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	bsh.Data = vsh.Data
	bsh.Len = vsh.Len * size
	bsh.Cap = vsh.Cap * size

	return b
}

// ByteSliceToFloat32 reinterprets a byte slice as a float32 slice (zero-copy).
// The byte slice length must be a multiple of 4. This is the symmetric inverse of
// Float32ToByteSlice and relies on the same native-endian memory layout.
func ByteSliceToFloat32(b []byte) []float32 {
	var ft float32
	size := int(reflect.TypeOf(ft).Size())

	var v []float32
	bsh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	vsh := (*reflect.SliceHeader)(unsafe.Pointer(&v))

	vsh.Data = bsh.Data
	vsh.Len = bsh.Len / size
	vsh.Cap = bsh.Cap / size

	return v
}
