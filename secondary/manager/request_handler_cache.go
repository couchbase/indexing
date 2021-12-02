// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package manager

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

const TEMP_FILE_SUFFIX string = ".tmp" // suffix for temporary filename to be atomically renamed

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
	metaCache map[string]*LocalIndexMetadata // IndexMetadata mem cache
	metaDir   string                         // metaCache persistence directory
	metaMutex sync.RWMutex                   // metaCache mutex

	// IndexStats subset memory cache
	statsCache map[string]*common.Statistics // IndexStats subset mem cache
	statsDir   string                        // statsCache persistence directory
	statsMutex sync.RWMutex                  // statsCache mutex

	// Channels for async caching to disk (runPersistor)
	workCh chan interface{} // incoming work for runPersistor (workChEntry_xxx types)
	doneCh chan bool        // closed to shut down runPersistor goroutine
}

// workChEntry_meta is an entry in the workCh channel for persisting a node's local index metadata.
type workChEntry_meta struct {
	hostKey string              // host:httpPort key to store under
	meta    *LocalIndexMetadata // entry to store
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

// NewRequestHandlerCache is the constructor for the requestHandlerCache class.
// cacheDir -- path to directory for the disk portion of the cache
func NewRequestHandlerCache(cacheDir string) *requestHandlerCache {
	this := &requestHandlerCache{} // constructed object to return

	// Create the memory caches
	this.metaCache = make(map[string]*LocalIndexMetadata)
	this.statsCache = make(map[string]*common.Statistics)

	// Create the disk cache directories (if not already there)
	this.metaDir = path.Join(cacheDir, "meta")
	this.statsDir = path.Join(cacheDir, "stats")
	os.MkdirAll(this.metaDir, 0755)
	os.MkdirAll(this.statsDir, 0755)

	// Launch the disk cache async persistor
	this.workCh = make(chan interface{}, 1024)
	this.doneCh = make(chan bool)
	go this.runPersistor()

	return this
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
func (this *requestHandlerCache) cacheLocalIndexMetadataBoot(hostKey string, meta *LocalIndexMetadata) {
	this.metaMutex.Lock()
	cached := this.metaCache[hostKey]
	if cached == nil || cached.Timestamp < meta.Timestamp {
		this.metaCache[hostKey] = meta
	}
	this.metaMutex.Unlock()
}

// CacheLocalIndexMetadata adds an entry to the local metadata memory cache and stages write-through
// to disk. hostKey is Host2key(host:httpPort).
func (this *requestHandlerCache) CacheLocalIndexMetadata(hostKey string, meta *LocalIndexMetadata) {
	// Memory cache (sync)
	this.metaMutex.Lock()
	this.metaCache[hostKey] = meta
	this.metaMutex.Unlock()

	// Disk cache (async)
	this.workCh <- &workChEntry_meta{hostKey, meta}
}

// cacheStatsBoot is called only at boot time to add an entry to the local IndexStats subset
// memory cache that was just read from the disk cache. This will be skipped if an entry for the
// given hostKey is already present as that would be from a newer getIndexStatus call.
func (this *requestHandlerCache) cacheStatsBoot(hostKey string, stats *common.Statistics) {
	this.statsMutex.Lock()
	if this.statsCache[hostKey] == nil {
		this.statsCache[hostKey] = stats
	}
	this.statsMutex.Unlock()
}

// CacheStats adds an entry to the local IndexStats subset memory cache and stages write-through to
// disk. hostKey is Host2key(host:httpPort).
func (this *requestHandlerCache) CacheStats(hostKey string, stats *common.Statistics) {
	// Memory cache (sync)
	this.statsMutex.Lock()
	this.statsCache[hostKey] = stats
	this.statsMutex.Unlock()

	// Disk cache (async)
	this.workCh <- &workChEntry_stats{hostKey, stats}
}

// GetAllCachedLocalIndexMetadata returns a clone of the metaCache map.
func (this *requestHandlerCache) GetAllCachedLocalIndexMetadata() map[string]*LocalIndexMetadata {
	clone := make(map[string]*LocalIndexMetadata) // return value

	this.metaMutex.RLock()
	for key, value := range this.metaCache {
		clone[key] = value
	}
	this.metaMutex.RUnlock()

	return clone
}

// GetLocalIndexMetadataFromCache looks up the cached LocalIndexMetadata for the given hostname from
// the memory cache. It does not need to check disk cache as nothing ever gets evicted from memory.
// hostKey is Host2key(host:httpPort).
func (this *requestHandlerCache) GetLocalIndexMetadataFromCache(hostKey string) *LocalIndexMetadata {
	const method = "requestHandlerCache::GetLocalIndexMetadataFromCache:" // for logging

	this.metaMutex.RLock()
	localMeta := this.metaCache[hostKey]
	this.metaMutex.RUnlock()
	if localMeta != nil && logging.IsEnabled(logging.Debug) {
		logging.Debugf("%v Found metadata in memory cache %v", method, hostKey)
	}
	return localMeta
}

// GetStatsFromCache looks up the cached subset of IndexStats for the given hostname from the memory
// cache. It does not need to check disk cache as nothing ever gets evicted from memory.
// hostKey is host2Key(host:httpPort).
func (this *requestHandlerCache) GetStatsFromCache(hostKey string) *common.Statistics {
	const method = "requestHandlerCache::GetStatsFromCache:" // for logging

	this.statsMutex.RLock()
	stats := this.statsCache[hostKey]
	this.statsMutex.RUnlock()
	if stats != nil && logging.IsEnabled(logging.Debug) {
		logging.Debugf("%v Found stats in memory cache %v", method, hostKey)
	}
	return stats
}

func (this *requestHandlerCache) saveLocalIndexMetadataToDisk(metaToCache *workChEntry_meta) error {
	const method = "requestHandlerCache::saveLocalIndexMetadataToDisk" // for logging

	if metaToCache.meta == nil {
		return nil
	}

	filepath := path.Join(this.metaDir, metaToCache.hostKey)
	temp := filepath + TEMP_FILE_SUFFIX

	content, err := json.Marshal(metaToCache.meta)
	if err != nil {
		logging.Errorf("%v Failed to marshal metadata to file %v, error: %v",
			method, filepath, err)
		return err
	}

	err = common.WriteFileWithSync(temp, content, 0755)
	if err != nil {
		logging.Errorf("%v Failed to save metadata to file %v, error: %v", method, temp, err)
		return err
	}

	err = os.Rename(temp, filepath)
	if err != nil {
		logging.Errorf("%v Failed to rename metadata to file %v, error: %v",
			method, filepath, err)
		return err
	}

	logging.Debugf("%v Successfully wrote metadata to disk for %v",
		method, metaToCache.hostKey)
	return nil
}

func (this *requestHandlerCache) saveStatsToDisk(statsToCache *workChEntry_stats) error {
	const method = "requestHandlerCache::saveStatsToDisk:" // for logging

	if statsToCache.stats == nil {
		return nil
	}

	filepath := path.Join(this.statsDir, statsToCache.hostKey)
	temp := filepath + TEMP_FILE_SUFFIX

	content, err := json.Marshal(statsToCache.stats)
	if err != nil {
		logging.Errorf("%v Failed to marshal stats to file %v, error: %v",
			method, filepath, err)
		return err
	}

	err = common.WriteFileWithSync(temp, content, 0755)
	if err != nil {
		logging.Errorf("%v Failed to save stats to file %v, error: %v", method, temp, err)
		return err
	}

	err = os.Rename(temp, filepath)
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
func (this *requestHandlerCache) DeleteObsoleteCacheEntries(keepKeys []string) {

	// Metadata memory cache (sync)
	this.metaMutex.Lock()
	for cacheKey := range this.metaCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(this.metaCache, cacheKey)
		}
	}
	this.metaMutex.Unlock()

	// Stats memory cache (sync)
	this.statsMutex.Lock()
	for cacheKey := range this.statsCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(this.statsCache, cacheKey)
		}
	}
	this.statsMutex.Unlock()

	// Disk metadata and stats caches (async). Add <entry>.tmp for each original keepKeys <entry>
	// because temp file is renamed via os.Rename which is async and may still be pending, so we
	// must avoid deleting the *.tmp cache file of a kept key.
	numKeys := len(keepKeys)
	for key := 0; key < numKeys; key++ {
		keepKeys = append(keepKeys, keepKeys[key]+TEMP_FILE_SUFFIX)
	}
	this.deleteObsoleteDiskCacheFiles(this.metaDir, keepKeys)
	this.deleteObsoleteDiskCacheFiles(this.statsDir, keepKeys)
}

// deleteObsoleteDiskCacheFiles takes a list of filenames to keep (keys and temp keys of Index nodes
// that currently exist) and deletes any files in the specified directory that are not in the list.
func (this *requestHandlerCache) deleteObsoleteDiskCacheFiles(dirPath string, keepFiles []string) {
	const method = "requestHandlerCache::deleteObsoleteDiskCacheFiles:" // for logging

	// Disk files that exist
	files, err := ioutil.ReadDir(dirPath)
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
			if err := os.RemoveAll(filepath); err != nil { // os.RemoveAll is atomic but *async*
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
func (this *requestHandlerCache) runPersistor() {
	const method = "requestHandlerCache::runPersistor:" // for logging

	// On startup, populate the memory caches from the disk caches. This is the only place disk
	// cache contents are read back into the memory caches.
	this.populateMetaMemCacheFromDisk()
	this.populateStatsMemCacheFromDisk()

	// Process incoming disk cache work until doneCh is closed
	for {
		select {

		case workChEntry, ok := <-this.workCh:
			if !ok {
				return
			}

			switch workChEntry.(type) {
			case *workChEntry_meta:
				this.saveLocalIndexMetadataToDisk(workChEntry.(*workChEntry_meta))
			case *workChEntry_stats:
				this.saveStatsToDisk(workChEntry.(*workChEntry_stats))
			case *workChEntry_keepKeys:
				this.DeleteObsoleteCacheEntries(workChEntry.(*workChEntry_keepKeys).keepKeys)
			default:
				logging.Errorf("%v Unsupported workChEntry type %T", method, workChEntry)
			}

		case <-this.doneCh:
			logging.Infof("%v Shutting down", method)
			return
		}
	}
}

// populateMetaMemCacheFromDisk is called at startup to initialze the LocalIndexMetadata memory
// cache from disk.
func (this *requestHandlerCache) populateMetaMemCacheFromDisk() {
	const _populateMetaMemCacheFromDisk = "requestHandlerCache::populateMetaMemCacheFromDisk"

	files, err := ioutil.ReadDir(this.metaDir)
	if err != nil {
		logging.Errorf("%v Failed to read metadata cache directory %v, error: %v",
			_populateMetaMemCacheFromDisk, this.metaDir, err)
	} else {
		for _, file := range files {
			hostKey := file.Name()
			filepath := path.Join(this.metaDir, hostKey)
			content, err := ioutil.ReadFile(filepath)
			if err != nil {
				logging.Errorf("%v Failed to read metadata from file %v, error: %v",
					_populateMetaMemCacheFromDisk, filepath, err)
			}

			localMeta := new(LocalIndexMetadata)
			if err := json.Unmarshal(content, localMeta); err != nil {
				logging.Errorf("%v Failed to unmarshal metadata from file %v, error: %v",
					_populateMetaMemCacheFromDisk, filepath, err)
			}

			// Add to mem cache if a new getIndexStatus call did not already cache this hostKey
			this.cacheLocalIndexMetadataBoot(hostKey, localMeta)
			logging.Debugf("%v Saved metadata to memory cache %v",
				_populateMetaMemCacheFromDisk, hostKey)
		}
	}
}

// populateStatsMemCacheFromDisk is called at startup to initialze the stats subset memory
// cache from disk.
func (this *requestHandlerCache) populateStatsMemCacheFromDisk() {
	const _populateStatsMemCacheFromDisk = "requestHandlerCache::populateStatsMemCacheFromDisk"

	files, err := ioutil.ReadDir(this.statsDir)
	if err != nil {
		logging.Errorf("%v Failed to read stats cache directory %v, error: %v",
			_populateStatsMemCacheFromDisk, this.statsDir, err)
	} else {
		for _, file := range files {
			hostKey := file.Name()
			filepath := path.Join(this.statsDir, hostKey)
			content, err := ioutil.ReadFile(filepath)
			if err != nil {
				logging.Errorf("%v Failed to read stats from file %v, error: %v",
					_populateStatsMemCacheFromDisk, filepath, err)
			}

			stats := new(common.Statistics)
			if err := json.Unmarshal(content, stats); err != nil {
				logging.Errorf("%v Failed to unmarshal stats from file %v, error: %v",
					_populateStatsMemCacheFromDisk, filepath, err)
			}

			// Add to mem cache if a new getIndexStatus call did not already cache this hostKey
			this.cacheStatsBoot(hostKey, stats)
			logging.Debugf("%v Saved stats to memory cache %v",
				_populateStatsMemCacheFromDisk, hostKey)
		}
	}
}

// Shutdown shuts down this requestHandlerCache object, which means terminating the goroutine that
// persists staged items to the disk cache.
func (this *requestHandlerCache) Shutdown() {
	close(this.doneCh)
}
