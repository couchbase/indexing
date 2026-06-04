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

func IndexPath2(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	return fmt.Sprintf("%s_%d_%d.index", inst.Defn.BucketUUID, instId, partnId)
}

func CodebookPath(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	indexPath := IndexPath(inst, partnId, sliceId)
	return filepath.Join(indexPath, CODEBOOK_DIR, CodebookName(inst, partnId, sliceId))
}

func CodebookName(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	return fmt.Sprintf("%s_%s_%d_%d.codebook", inst.Defn.Bucket, inst.Defn.Name, instId, partnId)
}

func CodebookName2(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	instId := inst.InstId
	if inst.IsProxy() {
		instId = inst.RealInstId
	}
	return fmt.Sprintf("%d_%d.codebook", instId, partnId)
}

func CodebookPath2(inst *common.IndexInst, partnId common.PartitionId, sliceId SliceId) string {
	indexPath := IndexPath2(inst, partnId, sliceId)
	return filepath.Join(indexPath, CODEBOOK_DIR, CodebookName2(inst, partnId, sliceId))
}

// Expected input is *.index/mainIndex or *.index/docIndex
func GetBucketUUIDIndexPath2(path string) string {
	path = filepath.Clean(path)
	path = strings.TrimSuffix(path, string(os.PathSeparator)+"mainIndex")
	path = strings.TrimSuffix(path, string(os.PathSeparator)+"docIndex")
	pathSlice := strings.Split(path, string(os.PathSeparator))
	if len(pathSlice) == 0 {
		return common.BUCKET_UUID_NIL
	}
	indexDir := pathSlice[len(pathSlice)-1]
	indexDirSlice := strings.Split(indexDir, "_")
	if len(indexDirSlice) == 0 {
		return common.BUCKET_UUID_NIL
	}
	return indexDirSlice[0]
}

func InitCodebookDir(
	storeEngineDir string, idxInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId,
) error {
	return InitCodebookDirAt(storeEngineDir, IndexPath2(idxInst, partnId, sliceId))
}

// InitCodebookDirAt creates the codebook subdirectory under an already-computed
// index path. Use this when the caller has already selected v1 or v2 layout.
func InitCodebookDirAt(storeEngineDir, relIndexPath string) error {
	codebookDirPath := filepath.Join(storeEngineDir, relIndexPath, CODEBOOK_DIR)
	if _, err := iowrap.Os_Stat(codebookDirPath); err != nil {
		if os.IsNotExist(err) {
			return iowrap.Os_MkdirAll(codebookDirPath, 0755)
		}
		logging.Errorf("InitCodebookDirAt: Error observed while checking the presence of "+
			"codebookDir: %v, err: %v", codebookDirPath, err)
		return err
	}
	return nil
}

func RemoveCodebookDir(
	storeEngineDir string, idxInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId,
) error {

	codebookDirPath := filepath.Join(
		storeEngineDir,
		IndexPath2(idxInst, partnId, sliceId),
		CODEBOOK_DIR,
	)

	oldCodebookDirPath := filepath.Join(
		storeEngineDir,
		IndexPath(idxInst, partnId, sliceId),
		CODEBOOK_DIR,
	)

	// Delete the oldCodebookDirPath too but it may not exist so don't return error if remove fails
	iowrap.Os_RemoveAll(oldCodebookDirPath)

	return iowrap.Os_RemoveAll(codebookDirPath)
}

/*
RemapCodebookDir handles the complexities of renaming codebook for supporting storage engines
during upgrades and is restart safe (aka can be called multiple times)

There are 2 storage engines to support: Plasma and Bhive.
Bhive has only dedicated instances and plasma has both shared + dedicated instances. We are always
going to have the new path directory created but the index data may/may not be in it depending on
the instance being shared or dedicated. in both cases codebook needs to be transferred to the new
directories if the instance had a codebook (aka it active and trained instances)

Remap goes as follows -
<storageDir>/"old_index_path".index/codebook/"bucket_index_partn".codebook
=>
<storageDir>/"new_index_path".index/codebook/"instID_partn".codebook

Its performed in 2 phases:
1. Update the `codebook` dir parent directory to the "new index path"
2. Rename the codebook file itself to the new file
*/
func RemapCodebookDir(
	storeEngineDir string, idxInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId,
	oldIndexPath string, newIndexPath string,
) error {

	if idxInst == nil {
		return fmt.Errorf("RemapCodebookDir: invalid instance for remapping codebook directory")
	}

	if oldIndexPath == newIndexPath {
		return nil
	}

	newCbDir := filepath.Join(newIndexPath, CODEBOOK_DIR)
	newCb := filepath.Join(newCbDir, CodebookName2(idxInst, partnId, sliceId))

	_, newCbExistsErr := iowrap.Os_Stat(newCb)
	if newCbExistsErr == nil {
		// codebook already exists at new path. return early
		logging.Infof("RemapCodebookDir: codebook already at new path for index %v. skipping",
			idxInst.DisplayName())
		return nil
	}

	// | oldCbDirExists	| newCbDirExists	| action								|
	// | true			| true				| likely rename not atomic. Skip rename |
	// | false			| true				| likely partial upgrade. Skip rename 	|
	// | true			| false				| rename 								|
	// | false			| false				| if inst active then codebook missing, |
	// | 				| 					| return err							|
	// | 				| 					| else likely failed training so ret nil|
	{
		////////// phase 1: verify if old index path has codebook dir or not
		oldCbDir := filepath.Join(oldIndexPath, CODEBOOK_DIR)

		_, oldPathErr := iowrap.Os_Stat(oldCbDir)
		_, newPathErr := iowrap.Os_Stat(newCbDir)
		if oldPathErr != nil {
			// old and new codebook dir does not exist; if the index is active then this is unexpected
			// unless we have crashed before deleting old slice dir
			if os.IsNotExist(oldPathErr) && os.IsNotExist(newPathErr) {
				if idxInst.State == common.INDEX_STATE_ACTIVE {
					err := fmt.Errorf("index %v state ACTIVE but no codebook found on old path - %w",
						idxInst.DisplayName(), oldPathErr)
					logging.Errorf("RemapCodebookDir: %v", err)

					return err
				}
				logging.Infof("RemapCodebook: inst %v not in ACTIVE state and no codebook exists. continue...",
					idxInst.DisplayName())
				return nil
			} else if os.IsNotExist(oldPathErr) && newPathErr == nil {
				// old codebook dir does not exist but new codebook dir does exist don't attempt
				// rename from [oldCbDir] to [newCbdir]
				logging.Infof("RemapCodebook: skip old codebook dir to new codebook dir rename for"+
					" index %v as old codebook dir does not exist",
					idxInst.DisplayName(),
				)
				goto phase2
			}

			err := fmt.Errorf("failed to stat codebook dir %v with err %w",
				oldCbDir, oldPathErr)

			logging.Errorf("RemapCodebook: %v", err)
			return err
		}

		// because we are using OS.rename APIs and we expect the paths to be on the same disk
		// the rename should be atomic (guaranteed on unix). so if the oldCbDir exists then
		// we don't expect the newCbDir to exist. but if it does due to other OSes not having
		// atomic renames then rename call below can fail so skip forward to phase 2
		if newPathErr == nil {
			logging.Infof("RemapCodebook: old and new codebook dir both exist for index %v. skipping rename...",
				idxInst.DisplayName())
			goto phase2
		}

		////////// phase 1: oldCbDir exists. call rename to update codebook's parent dir
		err := iowrap.Os_Rename(oldCbDir, newCbDir)
		if err != nil {
			err = fmt.Errorf("failed to remap codebook to %v with err %w", newCbDir, err)
			logging.Errorf("RemapCodebook: %v", err)
			return err
		}
	}

	////////// phase 1 guarantees newCbDir exists. now we need to rename old path format
	// to new format for the codebook file in phase 2
phase2:
	_, newPathErr := iowrap.Os_Stat(newCbDir)
	if newPathErr != nil {
		// newCbDir still does not exist. there's a likely issue with system
		err := fmt.Errorf("codebook dir couldn't be found at new path %v even after rename. err - %w",
			newIndexPath, newPathErr)
		logging.Errorf("RemapCodebookDir: %v", err)
		return err
	}

	////////// phase 2: rename codebook file
	oldCb := filepath.Join(newCbDir, CodebookName(idxInst, partnId, sliceId))

	err := iowrap.Os_Rename(oldCb, newCb)
	if err != nil {
		err = fmt.Errorf("failed to rename codebook file %v with err %w",
			newCb, err)
		logging.Errorf("RemapCodebookDir: %v", err)
		return err
	}

	logging.Infof("RemapCodebookDir: remapped codebook from %v to %v", oldCb, newCb)
	return nil
}

// This has to follow the pattern in IndexPath function defined above.
func GetIndexPathPattern() string {
	return "*_*_*_*.index"
}

// GetIndexPath2Pattern returns a glob pattern matching IndexPath2 format
// (bucketuuid_instid_partid.index). Since glob * matches underscores, this
// pattern also matches the old IndexPath format, so it covers both.
func GetIndexPath2Pattern() string {
	return "*_*_*.index"
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

func GetInstIdPartnIdFromPath2(idxPath string) (common.IndexInstId,
	common.PartitionId, error) {

	idxPath = strings.TrimPrefix(idxPath, common.BHIVE_DIR_PREFIX)

	isOldPath, _ := regexp.MatchString(".*_.*_[0-9]+_[0-9]+.index", idxPath)

	if isOldPath {
		return GetInstIdPartnIdFromPath(idxPath)
	}

	pathComponents := strings.Split(idxPath, "_")
	if len(pathComponents) < 2 {
		err := errors.New(fmt.Sprintf("Malformed index path %v", idxPath))
		return common.IndexInstId(0), common.PartitionId(0), err
	}

	// IndexPath2 format: bucketuuid_instid_partid.index (3 components)
	// Use len-2 so instId is correctly resolved for both formats.
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

// genKeyFileStagingName - don't use any prefix/suffix otherwise ns_server will fail importKeys
func genKeyFileStagingName(keyID string) string {
	return keyID
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

// utility function to remove old slice directory after remapping.
// a slice is remapped sequentially by its entries
func removeEmptySliceDir(slicePath string) error {
	entries, err := iowrap.Os_ReadDir(slicePath)
	switch {
	case err == nil && len(entries) > 0:
		return fmt.Errorf("%v old slice path has entries after remapping", slicePath)
	case err == nil:
		logging.Infof("%v removing old slice path after remapping", slicePath)
		return iowrap.Os_Remove(slicePath)
	case os.IsNotExist(err):
		return nil
	default:
		return fmt.Errorf("old slice path removal failed after remapping: %w", err)
	}
}

func GetBucketKDT(bucketUUID string) KeyDataType {
	return KeyDataType{
		TypeName:   kdtTypeServiceBucket,
		BucketUUID: bucketUUID,
	}
}
