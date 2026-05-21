// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

const TEMP_FILE_SUFFIX string = ".tmp" // suffix for temporary filename to be atomically renamed
const cacheFileMode os.FileMode = 0755 // permission bits for all cache files

var cacheMetaKDFLabel = []byte("indexing/request_handler_cache/meta")
var cacheStatsKDFLabel = []byte("indexing/request_handler_cache/stats")

var (
	errCacheEncryptedNoKeyLookup  = errors.New("cache file is encrypted but no key lookup available")
	errCacheKeyContextNotRestored = errors.New("cache file read ok but encryption key context not restored")
	errCacheShutdown              = errors.New("requestHandlerCache is shut down")
)

// encryption-related callbacks from EncryptionMgr.
type CacheEncryptionCallbacks struct {
	getKeyCipherById func(keyId string) ([]byte, string)
	setInUseKeys     func(kdt KeyDataType, key string)
}

// requestHandlerCache is a singleton child of requestHandlerContext and serves as its cache. This
// has both memory and disk components and currently caches LocalIndexMetadata and common.Statistics
// from all indexer nodes for /getIndexStatus and /getCachedIndexMetadata. New info is written into
// the memory cache first and eventually persisted to disk. Lookups check the memory cache first.
// The info for a given host is stored under key host:httpPort.
//
// Only the latest known full set of status data (i.e. for all indexes on a node) is cached for
// each node - partial sets are never cached.
type requestHandlerCache struct {

	// Index metadata memory cache
	metaCache      map[string]*manager.LocalIndexMetadata // IndexMetadata mem cache
	metaCacheReady bool                                   // is metaCache loaded from disk at boot yet?
	metaDir        string                                 // metaCache persistence directory
	metaMutex      sync.RWMutex                           // metaCache, metaCacheReady mutex

	// IndexStats subset memory cache
	statsCache      map[string]*common.Statistics // IndexStats subset mem cache
	statsCacheReady bool                          // is statsCache loaded from disk at boot yet?
	statsDir        string                        // statsCache persistence directory
	statsMutex      sync.RWMutex                  // statsCache, statsCacheReady mutex

	// Channels for async caching to disk (runPersistor)
	workCh  chan interface{} // incoming work for runPersistor (workChEntry_xxx types)
	doneCh  chan bool        // closed to shut down runPersistor goroutine
	readyCh chan struct{}    // closed by SetEncryptionCallbacks; blocks runPersistor until callbacks are set

	encCallbacksOnce sync.Once                // guards SetEncryptionCallbacks
	encCallbacks     CacheEncryptionCallbacks // callbacks from EncryptionMgr
	metaEncCtx       EncryptionCtx            // nil = plaintext; set = encrypt meta writes
	statsEncCtx      EncryptionCtx            // nil = plaintext; set = encrypt stats writes
	reportKeyInUse   func() error             // called after each encrypted write; nil in plaintext mode
}

// workChEntry_meta is an entry in the workCh channel for persisting a node's local index metadata.
type workChEntry_meta struct {
	hostKey string                      // host:httpPort key to store under
	meta    *manager.LocalIndexMetadata // entry to store
}

// workChEntry_stats is an entry in the workCh channel for persisting a node's statistics subset.
type workChEntry_stats struct {
	hostKey string             // host:httpPort key to store under
	stats   *common.Statistics // entry to store
}

// workChEntry_keepKeys is an entry in the workCh channel with a slice of cache keys to be kept. All
// keys not in this set are deleted from disk.
type workChEntry_keepKeys struct {
	keepKeys []string // slice of host:httpPort keys
}

// workChEntry_encryptUpdate is an entry in the workCh channel to update the encryption context
// for future writes (no file I/O). Enqueued by HandleEncryptionUpdateKey.
type workChEntry_encryptUpdate struct {
	earkey EaRKey
	kdt    KeyDataType
}

// workChEntry_encryptDrop is an entry in the workCh channel to encrypt, decrypt, or re-encrypt
// all cache files on disk. Enqueued by HandleEncryptionDropKey.
type workChEntry_encryptDrop struct {
	activeEarKey EaRKey
	dropKeyIds   []string
	kdt          KeyDataType
	respCh       chan error // closed by runPersistor after file I/O completes
}

// workChEntry_getInUseKeys is an entry in the workCh channel to collect the key IDs currently
// in use by the cache. Enqueued by HandleGetInUseKeys.
type workChEntry_getInUseKeys struct {
	kdt    KeyDataType
	respCh chan map[KeyDataType][]string
}

// NewRequestHandlerCache is the constructor for the requestHandlerCache class.
// cacheDir -- path to directory for the disk portion of the cache
func NewRequestHandlerCache(cacheDir string) *requestHandlerCache {
	rhc := &requestHandlerCache{} // constructed object to return

	// Create the memory caches
	rhc.metaCache = make(map[string]*manager.LocalIndexMetadata)
	rhc.statsCache = make(map[string]*common.Statistics)

	// Create the disk cache directories (if not already there)
	rhc.metaDir = path.Join(cacheDir, "meta")
	rhc.statsDir = path.Join(cacheDir, "stats")
	if err := iowrap.Os_MkdirAll(rhc.metaDir, cacheFileMode); err != nil {
		logging.Warnf("requestHandlerCache: MkdirAll %v: %v", rhc.metaDir, err)
	}
	if err := iowrap.Os_MkdirAll(rhc.statsDir, cacheFileMode); err != nil {
		logging.Warnf("requestHandlerCache: MkdirAll %v: %v", rhc.statsDir, err)
	}

	rhc.workCh = make(chan interface{}, 1024)
	rhc.doneCh = make(chan bool)
	rhc.readyCh = make(chan struct{})

	go rhc.runPersistor()
	return rhc
}

// SetEncryptionCallbacks supplies the encryption callbacks and unblocks the persistor
// Must be called exactly once after NewRequestHandlerCache.
func (rhc *requestHandlerCache) SetEncryptionCallbacks(encCallbacks CacheEncryptionCallbacks) {
	rhc.encCallbacksOnce.Do(func() {
		rhc.encCallbacks = encCallbacks
		close(rhc.readyCh)
	})
}

// Host2key converts a host:httpPort IPV4 or IPV6 string to a key for the metaCache and statsCache.
// This is also used as the filename for the disk halves of these caches.
func Host2key(hostname string) string {

	hostname = strings.Replace(hostname, ".", "_", -1)
	hostname = strings.Replace(hostname, ":", "_", -1)

	return hostname
}

// Key2host converts a host_httpPort cache key to the original IPV4 or IPV6 host:httpPort URL form,
// assuming the first char of an IPV6 key is "[" as this is required in URLs using IPV6.
func Key2host(hostKey string) string {
	if hostKey == "" {
		return ""
	}
	if hostKey[0:1] != "[" { // ipv4
		hostKey = strings.Replace(hostKey, "_", ".", 3)
		hostKey = strings.Replace(hostKey, "_", ":", 1)
	} else { // ipv6
		hostKey = strings.Replace(hostKey, "_", ":", -1)
	}
	return hostKey
}

// cacheLocalIndexMetadataBoot is called only at boot time to add an entry to the local metadata
// memory cache that was just read from the disk cache. This will be skipped if a newer entry for
// the given hostKey is already present as that would be from a newer getIndexStatus call. It will
// overwrite an older entry, which could exist if getIndexStats got that value from another node's
// cache that was older than the entry just read from this node's local disk cache.
func (rhc *requestHandlerCache) cacheLocalIndexMetadataBoot(hostKey string, meta *manager.LocalIndexMetadata) {
	rhc.metaMutex.Lock()
	cached := rhc.metaCache[hostKey]
	if cached == nil || cached.Timestamp < meta.Timestamp {
		rhc.metaCache[hostKey] = meta
	}
	rhc.metaMutex.Unlock()
}

// CacheLocalIndexMetadata adds an entry to the local metadata memory cache and stages write-through
// to disk. hostKey is Host2key(host:httpPort). This will be skipped if a newer entry for the given
// hostKey is already present as that would be from a newer getIndexStatus call.
func (rhc *requestHandlerCache) CacheLocalIndexMetadata(hostKey string, meta *manager.LocalIndexMetadata) {
	isNewer := false // is the meta arg newer than current cache contents?

	// Memory cache (sync)
	rhc.metaMutex.Lock()
	cached := rhc.metaCache[hostKey]
	if cached == nil || cached.Timestamp < meta.Timestamp {
		isNewer = true
		rhc.metaCache[hostKey] = meta
	}
	rhc.metaMutex.Unlock()

	// Disk cache (async)
	if isNewer {
		rhc.workCh <- &workChEntry_meta{hostKey, meta}
	}
}

// cacheStatsBoot is called only at boot time to add an entry to the local IndexStats subset
// memory cache that was just read from the disk cache. This will be skipped if an entry for the
// given hostKey is already present as that would be from a newer getIndexStatus call.
func (rhc *requestHandlerCache) cacheStatsBoot(hostKey string, stats *common.Statistics) {
	rhc.statsMutex.Lock()
	if rhc.statsCache[hostKey] == nil {
		rhc.statsCache[hostKey] = stats
	}
	rhc.statsMutex.Unlock()
}

// CacheStats adds an entry to the local IndexStats subset memory cache and stages write-through to
// disk. hostKey is Host2key(host:httpPort).
func (rhc *requestHandlerCache) CacheStats(hostKey string, stats *common.Statistics) {
	// Memory cache (sync)
	rhc.statsMutex.Lock()
	rhc.statsCache[hostKey] = stats
	rhc.statsMutex.Unlock()

	// Disk cache (async)
	rhc.workCh <- &workChEntry_stats{hostKey, stats}
}

// GetAllCachedLocalIndexMetadata returns a clone of the metaCache map and a boolean indicating
// whether the metaCache had fully finished loading from disk at boot, as one caller needs to be
// sure of that while another does not care.
func (rhc *requestHandlerCache) GetAllCachedLocalIndexMetadata() (
	clone map[string]*manager.LocalIndexMetadata, metaCacheReady bool) {
	clone = make(map[string]*manager.LocalIndexMetadata)

	rhc.metaMutex.RLock()
	for key, value := range rhc.metaCache {
		clone[key] = value
	}
	metaCacheReady = rhc.metaCacheReady
	rhc.metaMutex.RUnlock()

	return clone, metaCacheReady
}

// GetLocalIndexMetadataFromCache looks up the cached LocalIndexMetadata for the given hostname from
// the memory cache. It does not need to check disk cache as nothing ever gets evicted from memory.
// hostKey is Host2key(host:httpPort).
func (rhc *requestHandlerCache) GetLocalIndexMetadataFromCache(hostKey string) *manager.LocalIndexMetadata {
	const method = "requestHandlerCache::GetLocalIndexMetadataFromCache:" // for logging

	rhc.metaMutex.RLock()
	localMeta := rhc.metaCache[hostKey]
	rhc.metaMutex.RUnlock()
	if localMeta != nil && logging.IsEnabled(logging.Debug) {
		logging.Debugf("%v Found metadata in memory cache %v", method, hostKey)
	}
	return localMeta
}

// GetStatsFromCache looks up the cached subset of IndexStats for the given hostname from the memory
// cache. It does not need to check disk cache as nothing ever gets evicted from memory.
// hostKey is host2Key(host:httpPort).
func (rhc *requestHandlerCache) GetStatsFromCache(hostKey string) *common.Statistics {
	const method = "requestHandlerCache::GetStatsFromCache:" // for logging

	rhc.statsMutex.RLock()
	stats := rhc.statsCache[hostKey]
	rhc.statsMutex.RUnlock()
	if stats != nil && logging.IsEnabled(logging.Debug) {
		logging.Debugf("%v Found stats in memory cache %v", method, hostKey)
	}
	return stats
}

// writeTempFile writes content to temp, encrypted if ctx is non-nil, then calls reportKeyInUse.
func (rhc *requestHandlerCache) writeTempFile(method, temp string, content []byte, ctx EncryptionCtx) error {
	var err error
	if ctx != nil {
		if err = WriteEncryptedFile(temp, content, cacheFileMode, ctx, nil); err != nil {
			return fmt.Errorf("WriteEncryptedFile %v: %w", temp, err)
		}
	} else {
		if err = common.WriteFileWithSync(temp, content, cacheFileMode); err != nil {
			return fmt.Errorf("WriteFileWithSync %v: %w", temp, err)
		}
	}
	if rhc.reportKeyInUse != nil {
		if err = rhc.reportKeyInUse(); err != nil {
			logging.Errorf("%v reportKeyInUse: %v", method, err)
		}
	}
	return nil
}

func (rhc *requestHandlerCache) saveLocalIndexMetadataToDisk(metaToCache *workChEntry_meta) error {
	const method = "requestHandlerCache::saveLocalIndexMetadataToDisk"

	if metaToCache.meta == nil {
		return nil
	}

	filepath := path.Join(rhc.metaDir, metaToCache.hostKey)
	temp := filepath + TEMP_FILE_SUFFIX

	content, err := json.Marshal(metaToCache.meta)
	if err != nil {
		logging.Errorf("%v Failed to marshal metadata to file %v, error: %v", method, filepath, err)
		return err
	}

	if err = rhc.writeTempFile(method, temp, content, rhc.metaEncCtx); err != nil {
		logging.Errorf("%v Failed to write metadata to file %v, error: %v", method, temp, err)
		return err
	}

	if err = iowrap.Os_Rename(temp, filepath); err != nil {
		logging.Errorf("%v Failed to rename metadata to file %v, error: %v", method, filepath, err)
		return err
	}

	logging.Debugf("%v Successfully wrote metadata to disk for %v", method, metaToCache.hostKey)
	return nil
}

func (rhc *requestHandlerCache) saveStatsToDisk(statsToCache *workChEntry_stats) error {
	const method = "requestHandlerCache::saveStatsToDisk"

	if statsToCache.stats == nil {
		return nil
	}

	filepath := path.Join(rhc.statsDir, statsToCache.hostKey)
	temp := filepath + TEMP_FILE_SUFFIX

	content, err := json.Marshal(statsToCache.stats)
	if err != nil {
		logging.Errorf("%v Failed to marshal stats to file %v, error: %v", method, filepath, err)
		return err
	}

	if err = rhc.writeTempFile(method, temp, content, rhc.statsEncCtx); err != nil {
		logging.Errorf("%v Failed to write stats to file %v, error: %v", method, temp, err)
		return err
	}

	err = iowrap.Os_Rename(temp, filepath)
	if err != nil {
		logging.Errorf("%v Failed to rename stats to file %v, error: %v",
			method, filepath, err)
		return err
	}

	logging.Debugf("%v Successfully wrote stats to disk for %v",
		method, statsToCache.hostKey)
	return nil
}

// DeleteObsoleteCacheEntries deletes obsolete entries from all getIndexStatus memory caches and
// stages deletion of same from the disk caches.
// keepKeys correspond to the current indexer nodes; all other entries are deleted.
func (rhc *requestHandlerCache) DeleteObsoleteCacheEntries(keepKeys []string) {
	// Metadata memory cache (sync)
	rhc.metaMutex.Lock()
	for cacheKey := range rhc.metaCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(rhc.metaCache, cacheKey)
		}
	}
	rhc.metaMutex.Unlock()

	// Stats memory cache (sync)
	rhc.statsMutex.Lock()
	for cacheKey := range rhc.statsCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(rhc.statsCache, cacheKey)
		}
	}
	rhc.statsMutex.Unlock()

	// Disk metadata and stats caches (async). Add <entry>.tmp for each original keepKeys <entry>
	// because temp file is renamed via iowrap.Os_Rename which is async and may still be pending, so
	// we must avoid deleting the *.tmp cache file of a kept key.
	numKeys := len(keepKeys)
	for key := 0; key < numKeys; key++ {
		keepKeys = append(keepKeys, keepKeys[key]+TEMP_FILE_SUFFIX)
	}
	rhc.deleteObsoleteDiskCacheFiles(rhc.metaDir, keepKeys)
	rhc.deleteObsoleteDiskCacheFiles(rhc.statsDir, keepKeys)
}

// deleteObsoleteDiskCacheFiles takes a list of filenames to keep (keys and temp keys of Index nodes
// that currently exist) and deletes any files in the specified directory that are not in the list.
func (rhc *requestHandlerCache) deleteObsoleteDiskCacheFiles(dirPath string, keepFiles []string) {
	const method = "requestHandlerCache::deleteObsoleteDiskCacheFiles:" // for logging

	// Disk files that exist
	files, err := iowrap.Ioutil_ReadDir(dirPath)
	if err != nil {
		logging.Errorf("%v Failed to read directory %v, error: %v", method, dirPath, err)
		return
	}

	for _, file := range files {
		filename := file.Name()

		found := false
		for _, hostKey := range keepFiles {
			if hostKey == filename {
				found = true
				break
			}
		}

		if !found {
			filepath := path.Join(dirPath, filename)
			if err := iowrap.Os_RemoveAll(filepath); err != nil { // atomic but *async*
				logging.Errorf("%v Failed to remove file %v, error: %v",
					method, filepath, err)
			} else if logging.IsEnabled(logging.Debug) {
				logging.Debugf("%v Successfully removed file %v.", method, filepath)
			}
		}
	}
}

///////////////////////////////////////////////////////
// persistor
///////////////////////////////////////////////////////

// runPersistor runs in a Go routine and persists IndexStatus data
// from all indexer nodes to local disk cache.
func (rhc *requestHandlerCache) runPersistor() {
	const method = "requestHandlerCache::runPersistor:" // for logging

	// Block until SetEncryptionCallbacks is called so that encCallbacks are available
	// before any disk reads (encrypted files need getKeyCipherById to decrypt).
	select {
	case <-rhc.readyCh:
	case <-rhc.doneCh:
		return
	}

	// On startup, populate the memory caches from the disk caches. This is the only place disk
	// cache contents are read back into the memory caches.
	rhc.populateMetaMemCacheFromDisk()
	rhc.populateStatsMemCacheFromDisk()

	// Process incoming disk cache work until doneCh is closed
	for {
		select {

		case workChEntry, ok := <-rhc.workCh:
			if !ok {
				return
			}

			switch e := workChEntry.(type) {
			case *workChEntry_meta:
				rhc.saveLocalIndexMetadataToDisk(e)
			case *workChEntry_stats:
				rhc.saveStatsToDisk(e)
			case *workChEntry_keepKeys:
				rhc.DeleteObsoleteCacheEntries(e.keepKeys)
			case *workChEntry_encryptUpdate:
				rhc.doEncryptionUpdateKey(e)
			case *workChEntry_encryptDrop:
				rhc.doEncryptionDropKey(e)
			case *workChEntry_getInUseKeys:
				rhc.doGetInUseKeys(e)
			default:
				logging.Errorf("%v Unsupported workChEntry type %T", method, workChEntry)
			}

		case <-rhc.doneCh:
			logging.Infof("%v Shutting down", method)
			return
		}
	}
}

// common code for both stats and meta cache to read a file,
// detect if it's encrypted, and if so use the callbacks to
//
//	decrypt it and build an encryption context for future writes.
func (rhc *requestHandlerCache) readFile(filepath string, kdfLabel []byte) ([]byte, EncryptionCtx, error) {
	isEncrypted, encErr := IsFileEncrypted(filepath)
	if encErr != nil || !isEncrypted {
		// Plaintext, or header unreadable — try plain read either way.
		content, err := iowrap.Ioutil_ReadFile(filepath)
		if err != nil {
			return nil, nil, fmt.Errorf("Ioutil_ReadFile %v: %w", filepath, err)
		}
		return content, nil, nil
	}

	if rhc.encCallbacks.getKeyCipherById == nil {
		return nil, nil, errCacheEncryptedNoKeyLookup
	}

	var resolvedKeyID string
	var resolvedKeyData []byte
	var resolvedCipher string
	getKey := func(keyId []byte) []byte {
		defer func() {
			if r := recover(); r != nil {
				logging.Errorf("requestHandlerCache::readFile: key lookup panicked for %s: %v",
					string(keyId), r)
			}
		}()
		data, cipher := rhc.encCallbacks.getKeyCipherById(string(keyId))
		resolvedKeyID = string(keyId)
		resolvedKeyData = data
		resolvedCipher = cipher
		return data
	}

	content, err := ReadEncryptedFile(filepath, getKey, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadEncryptedFile %v: %w", filepath, err)
	}
	if resolvedKeyID == "" || len(resolvedKeyData) == 0 || resolvedCipher == "" {
		return nil, nil, errCacheKeyContextNotRestored
	}
	ctx, ctxErr := rhc.buildEncryptionContextForKey(resolvedKeyID, resolvedKeyData, resolvedCipher, kdfLabel)
	if ctxErr != nil {
		return nil, nil, ctxErr
	}
	return content, ctx, nil
}

// populateMetaMemCacheFromDisk is called at startup to initialze the LocalIndexMetadata memory
// cache from disk.
func (rhc *requestHandlerCache) populateMetaMemCacheFromDisk() {
	const method = "requestHandlerCache::populateMetaMemCacheFromDisk"

	files, err := iowrap.Ioutil_ReadDir(rhc.metaDir)
	if err != nil {
		logging.Errorf("%v Failed to read metadata cache directory %v, error: %v",
			method, rhc.metaDir, err)
	} else {
		for _, file := range files {
			hostKey := file.Name()
			if strings.HasSuffix(hostKey, TEMP_FILE_SUFFIX) {
				continue
			}

			filepath := path.Join(rhc.metaDir, hostKey)
			content, encCtx, err := rhc.readFile(filepath, cacheMetaKDFLabel)
			if err != nil {
				logging.Errorf("%v Failed to read metadata file %v, error: %v", method, filepath, err)
				continue
			}
			if encCtx != nil {
				rhc.metaEncCtx = encCtx
			}

			localMeta := new(manager.LocalIndexMetadata)
			if err := json.Unmarshal(content, localMeta); err != nil {
				logging.Errorf("%v Failed to unmarshal metadata from file %v, error: %v",
					method, filepath, err)
				continue
			}

			// Add to mem cache if a new getIndexStatus call did not already cache this hostKey
			rhc.cacheLocalIndexMetadataBoot(hostKey, localMeta)
			logging.Debugf("%v Saved metadata to memory cache %v", method, hostKey)
		}
	}

	// Mark cache as ready even if something failed. It will get populated by next getIndexStatus.
	rhc.metaMutex.Lock()
	rhc.metaCacheReady = true
	rhc.metaMutex.Unlock()
}

// populateStatsMemCacheFromDisk is called at startup to initialze the stats subset memory
// cache from disk.
func (rhc *requestHandlerCache) populateStatsMemCacheFromDisk() {
	const method = "requestHandlerCache::populateStatsMemCacheFromDisk"

	files, err := iowrap.Ioutil_ReadDir(rhc.statsDir)
	if err != nil {
		logging.Errorf("%v Failed to read stats cache directory %v, error: %v",
			method, rhc.statsDir, err)
	} else {
		for _, file := range files {
			hostKey := file.Name()
			if strings.HasSuffix(hostKey, TEMP_FILE_SUFFIX) {
				continue
			}

			filepath := path.Join(rhc.statsDir, hostKey)
			content, encCtx, err := rhc.readFile(filepath, cacheStatsKDFLabel)
			if err != nil {
				logging.Errorf("%v Failed to read stats file %v, error: %v", method, filepath, err)
				continue
			}
			if encCtx != nil {
				rhc.statsEncCtx = encCtx
			}

			stats := new(common.Statistics)
			if err := json.Unmarshal(content, stats); err != nil {
				logging.Errorf("%v Failed to unmarshal stats from file %v, error: %v",
					method, filepath, err)
				continue
			}

			// Add to mem cache if a new getIndexStatus call did not already cache this hostKey
			rhc.cacheStatsBoot(hostKey, stats)
			logging.Debugf("%v Saved stats to memory cache %v", method, hostKey)
		}
	}

	// Mark cache as ready even if something failed. It will get populated by next getIndexStatus.
	rhc.statsMutex.Lock()
	rhc.statsCacheReady = true
	rhc.statsMutex.Unlock()
}

func (rhc *requestHandlerCache) createEncryptionContext(
	earkey EaRKey, kdfLabel []byte) (EncryptionCtx, error) {

	ctx, err := NewEncryptionCtx(earkey.Id, earkey.Key, earkey.Cipher, kdfLabel)
	if err != nil {
		return nil, fmt.Errorf("requestHandlerCache::createEncryptionContext: %w", err)
	}
	return ctx, nil
}

func (rhc *requestHandlerCache) buildEncryptionContextForKey(
	keyID string, keyData []byte, cipher string, kdfLabel []byte) (EncryptionCtx, error) {

	ctx, err := NewEncryptionCtx(keyID, keyData, cipher, kdfLabel)
	if err != nil {
		return nil, fmt.Errorf("requestHandlerCache::buildEncryptionContextForKey: %w", err)
	}
	return ctx, nil
}

// HandleEncryptionUpdateKey enqueues an encryption context update onto workCh so it is
// processed by the runPersistor goroutine.
// Called by the indexer's handleEncryptionUpdateKey for the "log" KeyDataType.
// The send is done in a goroutine so it never blocks the indexer event loop.
func (rhc *requestHandlerCache) HandleEncryptionUpdateKey(earkey EaRKey, kdt KeyDataType) {
	entry := &workChEntry_encryptUpdate{earkey: earkey, kdt: kdt}
	go func() {
		select {
		case rhc.workCh <- entry:
		case <-rhc.doneCh:
		}
	}()
}

// doEncryptionUpdateKey updates metaEncCtx and statsEncCtx for future writes.
// No file re-encryption happens here — that is handled by doEncryptionDropKey.
func (rhc *requestHandlerCache) doEncryptionUpdateKey(e *workChEntry_encryptUpdate) {
	const method = "requestHandlerCache::doEncryptionUpdateKey"

	metaCtx, err := rhc.createEncryptionContext(e.earkey, cacheMetaKDFLabel)
	if err != nil {
		logging.Errorf("%v meta context: %v", method, err)
		return
	}
	statsCtx, err := rhc.createEncryptionContext(e.earkey, cacheStatsKDFLabel)
	if err != nil {
		logging.Errorf("%v stats context: %v", method, err)
		return
	}

	if metaCtx == nil {
		// Encryption is being disabled. Do not clear the write contexts here — the files on
		// disk are still encrypted. doEncryptionDropKey will decrypt them and then clear the
		// contexts. If we nil the contexts now, doEncryptionDropKey would see currentKeyId==""
		// and incorrectly skip decryption, leaving encrypted files on disk permanently.
		logging.Infof("%v encryption disabled, deferring context clear to drop key", method)
		return
	}

	rhc.metaEncCtx = metaCtx
	rhc.statsEncCtx = statsCtx
	rhc.reportKeyInUse = func() error {
		if rhc.encCallbacks.setInUseKeys == nil {
			return nil
		}
		rhc.encCallbacks.setInUseKeys(e.kdt, e.earkey.Id)
		return nil
	}

	logging.Infof("%v key:%v", method, e.earkey.Id)
}

// HandleEncryptionDropKey enqueues a re-encrypt/decrypt operation and returns a channel
// that is sent on by runPersistor when all file I/O is done. The caller must wait on
// this channel before signalling cbauth.KeysDropComplete so that the old key is not
// evicted before re-encryption reads are finished. The indexer goroutine must NOT
// block on this channel directly; it should spawn a separate goroutine to wait on it.
func (rhc *requestHandlerCache) HandleEncryptionDropKey(
	activeEarKey EaRKey, dropKeyIds []string, kdt KeyDataType) <-chan error {

	respCh := make(chan error, 1)
	entry := &workChEntry_encryptDrop{
		activeEarKey: activeEarKey,
		dropKeyIds:   dropKeyIds,
		kdt:          kdt,
		respCh:       respCh,
	}
	go func() {
		select {
		case rhc.workCh <- entry:
		case <-rhc.doneCh:
			entry.respCh <- errCacheShutdown
		}
	}()
	return respCh
}

// doEncryptionDropKey handles the three key-lifecycle scenarios
// for all files in both cache directories.

// Scenario 1 (activeEarKey set, no dropKeyIds): first-time encrypt of plaintext files.
// Scenario 2 (no activeEarKey, dropKeyIds set): decrypt back to plaintext.
// Scenario 3 (activeEarKey set, dropKeyIds set): re-encrypt from old key(s) to new key.
func (rhc *requestHandlerCache) doEncryptionDropKey(e *workChEntry_encryptDrop) {
	const method = "requestHandlerCache::doEncryptionDropKey"

	emptyDropKeys := len(e.dropKeyIds) == 0 || (len(e.dropKeyIds) == 1 && e.dropKeyIds[0] == "")
	currentKeyId := ""
	if rhc.metaEncCtx != nil {
		currentKeyId = string(rhc.metaEncCtx.KeyID())
	}
	var err error
	switch {
	case e.activeEarKey.Id != "" && emptyDropKeys:
		err = rhc.encryptAllCacheFiles(e.activeEarKey, e.kdt)
	case e.activeEarKey.Id == "" && len(e.dropKeyIds) > 0:
		if currentKeyId == "" {
			logging.Infof("%v already plaintext, skipping", method)
			e.respCh <- nil
			return
		}
		err = rhc.decryptAllCacheFiles(e.kdt)
	case e.activeEarKey.Id != "" && len(e.dropKeyIds) > 0:
		// Always re-encrypt: metaEncCtx tracks the write context, not what key the files on
		// disk actually have. Old files (written with a previous key) may still exist on disk
		// even after ENCRYPTION_UPDATE_KEY updated the write context. reencryptDirFiles is
		// idempotent — ReencryptFileByChunk returns n=0 if the file is already on the target key.
		err = rhc.reencryptAllCacheFiles(e.activeEarKey, e.kdt)
	default:
		logging.Warnf("%v no-op: activeKey=%q dropKeyIds=%v", method, e.activeEarKey.Id, e.dropKeyIds)
		e.respCh <- nil
		return
	}

	if err != nil {
		logging.Errorf("%v error: %v", method, err)
	}
	e.respCh <- err
}

// HandleGetInUseKeys enqueues a query for the key IDs currently in use by the cache.
// The result is sent on respCh by runPersistor.
func (rhc *requestHandlerCache) HandleGetInUseKeys(kdt KeyDataType, respCh chan map[KeyDataType][]string) {
	entry := &workChEntry_getInUseKeys{kdt: kdt, respCh: respCh}
	go func() {
		select {
		case rhc.workCh <- entry:
		case <-rhc.doneCh:
			entry.respCh <- nil
		}
	}()
}

func (rhc *requestHandlerCache) doGetInUseKeys(e *workChEntry_getInUseKeys) {
	result := make(map[KeyDataType][]string)
	if rhc.metaEncCtx != nil {
		result[e.kdt] = []string{string(rhc.metaEncCtx.KeyID())}
	}
	e.respCh <- result
}

// encryptAllCacheFiles encrypts all existing plaintext files in meta/ and stats/ dirs.
func (rhc *requestHandlerCache) encryptAllCacheFiles(activeKey EaRKey, kdt KeyDataType) error {
	const method = "requestHandlerCache::encryptAllCacheFiles"

	metaCtx, err := rhc.createEncryptionContext(activeKey, cacheMetaKDFLabel)
	if err != nil {
		return fmt.Errorf("%v meta context: %w", method, err)
	}
	statsCtx, err := rhc.createEncryptionContext(activeKey, cacheStatsKDFLabel)
	if err != nil {
		return fmt.Errorf("%v stats context: %w", method, err)
	}

	if err = rhc.encryptDirFiles(rhc.metaDir, metaCtx, cacheMetaKDFLabel); err != nil {
		return fmt.Errorf("%v metaDir: %w", method, err)
	}
	if err = rhc.encryptDirFiles(rhc.statsDir, statsCtx, cacheStatsKDFLabel); err != nil {
		return fmt.Errorf("%v statsDir: %w", method, err)
	}

	rhc.metaEncCtx = metaCtx
	rhc.statsEncCtx = statsCtx
	rhc.reportKeyInUse = func() error {
		if rhc.encCallbacks.setInUseKeys == nil {
			return nil
		}
		rhc.encCallbacks.setInUseKeys(kdt, activeKey.Id)
		return nil
	}
	if err = rhc.reportKeyInUse(); err != nil {
		logging.Errorf("%v reportKeyInUse: %v", method, err)
	}
	logging.Infof("%v encrypted with key:%v", method, activeKey.Id)
	return nil
}

// decryptAllCacheFiles decrypts all encrypted files in meta/ and stats/ dirs back to plaintext.
func (rhc *requestHandlerCache) decryptAllCacheFiles(kdt KeyDataType) error {
	const method = "requestHandlerCache::decryptAllCacheFiles"

	if err := rhc.decryptDirFiles(rhc.metaDir, cacheMetaKDFLabel); err != nil {
		return fmt.Errorf("%v metaDir: %w", method, err)
	}
	if err := rhc.decryptDirFiles(rhc.statsDir, cacheStatsKDFLabel); err != nil {
		return fmt.Errorf("%v statsDir: %w", method, err)
	}

	rhc.metaEncCtx = nil
	rhc.statsEncCtx = nil
	rhc.reportKeyInUse = func() error {
		if rhc.encCallbacks.setInUseKeys == nil {
			return nil
		}
		rhc.encCallbacks.setInUseKeys(kdt, "")
		return nil
	}
	logging.Infof("%v decrypted to plaintext", method)
	return nil
}

func (rhc *requestHandlerCache) reencryptAllCacheFiles(activeKey EaRKey, kdt KeyDataType) error {
	const method = "requestHandlerCache::reencryptAllCacheFiles"

	newMetaCtx, err := rhc.createEncryptionContext(activeKey, cacheMetaKDFLabel)
	if err != nil {
		return fmt.Errorf("%v meta context: %w", method, err)
	}
	newStatsCtx, err := rhc.createEncryptionContext(activeKey, cacheStatsKDFLabel)
	if err != nil {
		return fmt.Errorf("%v stats context: %w", method, err)
	}

	if err = rhc.reencryptDirFiles(rhc.metaDir, newMetaCtx, cacheMetaKDFLabel); err != nil {
		return fmt.Errorf("%v metaDir: %w", method, err)
	}
	if err = rhc.reencryptDirFiles(rhc.statsDir, newStatsCtx, cacheStatsKDFLabel); err != nil {
		return fmt.Errorf("%v statsDir: %w", method, err)
	}

	rhc.metaEncCtx = newMetaCtx
	rhc.statsEncCtx = newStatsCtx
	rhc.reportKeyInUse = func() error {
		if rhc.encCallbacks.setInUseKeys == nil {
			return nil
		}
		rhc.encCallbacks.setInUseKeys(kdt, activeKey.Id)
		return nil
	}
	if err = rhc.reportKeyInUse(); err != nil {
		logging.Errorf("%v reportKeyInUse: %v", method, err)
	}
	return nil
}

// decryptDirFiles decrypts every encrypted (non-.tmp) file in dirPath back to plaintext.
// kdfLabel must match the label used when the files were encrypted.
func (rhc *requestHandlerCache) decryptDirFiles(dirPath string, kdfLabel []byte) error {
	const method = "requestHandlerCache::decryptDirFiles"

	getKeyById := rhc.makeGetKeyByIdFunc()
	files, err := iowrap.Ioutil_ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("%v ReadDir %v: %w", method, dirPath, err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), TEMP_FILE_SUFFIX) {
			continue
		}
		fp := path.Join(dirPath, file.Name())
		temp := fp + TEMP_FILE_SUFFIX

		isEncrypted, checkErr := IsFileEncrypted(fp)
		if checkErr != nil || !isEncrypted {
			continue // plaintext or unreadable — nothing to decrypt
		}

		if _, err = DecryptFileByChunk(context.TODO(),
			fp, temp, getKeyById, kdfLabel, nil); err != nil {
			return fmt.Errorf("%v DecryptFileByChunk %v: %w", method, fp, err)
		}
		if err = iowrap.Os_Rename(temp, fp); err != nil {
			return fmt.Errorf("%v Rename %v: %w", method, fp, err)
		}
	}
	return nil
}

// encryptDirFiles encrypts every (non-.tmp) plaintext file in dirPath with ctx.
// Files already on the target key are skipped; files on a different key are re-encrypted.
func (rhc *requestHandlerCache) encryptDirFiles(
	dirPath string, ctx EncryptionCtx, kdfLabel []byte) error {

	return rhc.encryptOrReencryptDirFiles("requestHandlerCache::encryptDirFiles", dirPath, ctx, kdfLabel)
}

// reencryptDirFiles re-encrypts every file in dirPath to newCtx.
// Plaintext files surviving from a crash mid-encryption are also encrypted.
func (rhc *requestHandlerCache) reencryptDirFiles(
	dirPath string, newCtx EncryptionCtx, kdfLabel []byte) error {

	return rhc.encryptOrReencryptDirFiles("requestHandlerCache::reencryptDirFiles", dirPath, newCtx, kdfLabel)
}

// encryptOrReencryptDirFiles is the shared implementation for encryptDirFiles and reencryptDirFiles.
// method is the caller name used in error/log messages to keep them distinct.
func (rhc *requestHandlerCache) encryptOrReencryptDirFiles(
	method, dirPath string, ctx EncryptionCtx, kdfLabel []byte) error {

	getKeyById := rhc.makeGetKeyByIdFunc()
	files, err := iowrap.Ioutil_ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("%v ReadDir %v: %w", method, dirPath, err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), TEMP_FILE_SUFFIX) {
			continue
		}
		fp := path.Join(dirPath, file.Name())
		temp := fp + TEMP_FILE_SUFFIX

		isEncrypted, checkErr := IsFileEncrypted(fp)
		switch {
		case checkErr != nil:
			return fmt.Errorf("%v IsFileEncrypted %v: %w", method, fp, checkErr)

		case isEncrypted:
			var n uint64
			if n, err = ReencryptFileByChunk(context.TODO(),
				fp, temp, ctx, getKeyById, kdfLabel, nil); err != nil {
				return fmt.Errorf("%v ReencryptFileByChunk %v: %w", method, fp, err)
			}
			if n == 0 {
				// already encrypted with the target key — no temp file was created
				continue
			}

		default:
			content, readErr := iowrap.Ioutil_ReadFile(fp)
			if readErr != nil {
				if os.IsNotExist(readErr) {
					continue
				}
				return fmt.Errorf("%v ReadFile %v: %w", method, fp, readErr)
			}
			if err = WriteEncryptedFile(temp, content, cacheFileMode, ctx, nil); err != nil {
				return fmt.Errorf("%v WriteEncryptedFile %v: %w", method, temp, err)
			}
		}

		if err = iowrap.Os_Rename(temp, fp); err != nil {
			return fmt.Errorf("%v Rename %v: %w", method, fp, err)
		}
	}
	return nil
}

// makeGetKeyByIdFunc returns a key-lookup closure backed by encCallbacks.getKeyCipherById.
func (rhc *requestHandlerCache) makeGetKeyByIdFunc() func([]byte) []byte {
	return func(keyId []byte) []byte {
		if rhc.encCallbacks.getKeyCipherById == nil {
			return nil
		}
		defer func() {
			if r := recover(); r != nil {
				logging.Errorf("requestHandlerCache::makeGetKeyByIdFunc key lookup panicked for %s: %v",
					string(keyId), r)
			}
		}()
		data, _ := rhc.encCallbacks.getKeyCipherById(string(keyId))
		return data
	}
}

// Shutdown shuts down this requestHandlerCache object, which means terminating the goroutine that
// persists staged items to the disk cache.
func (rhc *requestHandlerCache) Shutdown() {
	close(rhc.doneCh)
}
