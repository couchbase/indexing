// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manager

import (
	"fmt"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/planner"
)

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

type RestoreContext struct {
	clusterUrl   string
	image        *ClusterIndexMetadata
	current      *planner.Plan
	target       string
	idxFromImage map[common.IndexerId][]*planner.IndexUsage
	idxToRestore map[common.IndexerId][]*planner.IndexUsage
	indexerMap   map[common.IndexerId]common.IndexerId
	filters      map[string]bool
	filterType   string
	remap        map[string]string
	schedTokens  map[common.IndexDefnId]*mc.ScheduleCreateToken
	tokToRestore map[common.IndexDefnId]*mc.ScheduleCreateToken
	defnInImage  map[common.IndexDefnId]bool
	origBucket   map[string]bool
	instNameMap  map[string]*planner.IndexUsage
}

//////////////////////////////////////////////////////////////
// RestoreContext
//////////////////////////////////////////////////////////////

//
// Initialize restore context
//
func createRestoreContext(image *ClusterIndexMetadata, clusterUrl string, bucket string,
	filters map[string]bool, filterType string, remap map[string]string) *RestoreContext {

	context := &RestoreContext{
		clusterUrl:   clusterUrl,
		image:        image,
		target:       bucket,
		idxFromImage: make(map[common.IndexerId][]*planner.IndexUsage),
		idxToRestore: make(map[common.IndexerId][]*planner.IndexUsage),
		indexerMap:   make(map[common.IndexerId]common.IndexerId),
		filters:      filters,
		filterType:   filterType,
		remap:        remap,
		defnInImage:  make(map[common.IndexDefnId]bool),
		origBucket:   make(map[string]bool),
		tokToRestore: make(map[common.IndexDefnId]*mc.ScheduleCreateToken),
		instNameMap:  make(map[string]*planner.IndexUsage),
	}

	return context
}

//
// Restore and place index in the image onto the current cluster.
//
func (m *RestoreContext) computeIndexLayout() (map[string][]*common.IndexDefn, error) {

	// convert storage mode
	if err := m.convertStorageMode(); err != nil {
		return nil, err
	}

	// convert image to IndexUsage
	if err := m.convertImage(); err != nil {
		return nil, err
	}

	// cleanse the image
	m.cleanseBackupMetadata()

	// Fetch the index layout from current cluster
	current, err := planner.RetrievePlanFromCluster(m.clusterUrl, nil, false)
	if err != nil {
		return nil, err
	}
	m.current = current

	// Get schedule create tokens from current cluster
	var schedTokens map[common.IndexDefnId]*mc.ScheduleCreateToken
	schedTokens, err = getSchedCreateTokens(m.target, m.filters, m.filterType)
	if err != nil {
		return nil, err
	}
	m.schedTokens = schedTokens

	m.prepareInstNameMap()

	// find index to restore
	if err := m.findIndexToRestore(); err != nil {
		return nil, err
	}

	// find schedule create tokens to restore.
	if err := m.findSchedTokensToRestore(); err != nil {
		return nil, err
	}

	if len(m.idxToRestore) == 0 && len(m.tokToRestore) == 0 {
		logging.Infof("RestoreContext: nothing to restore")
		return nil, nil
	}

	// associate indexer from image to current cluster
	m.buildIndexerMapping()

	// build index defnition map
	if err := m.updateIndexDefnId(); err != nil {
		return nil, err
	}

	// invoke placement
	return m.placeIndex()
}

//
// Convert storage mode of index to cluster storage mode
//
func (m *RestoreContext) convertStorageMode() error {

	for i, _ := range m.image.Metadata {
		meta := &m.image.Metadata[i]
		for j, _ := range meta.IndexDefinitions {
			defn := &meta.IndexDefinitions[j]
			defn.Using = "gsi"
		}
	}

	for _, token := range m.image.SchedTokens {
		token.Definition.Using = "gsi"
	}

	return nil
}

//
// Convert from LocalIndexMetadata to IndexUsage
//
func (m *RestoreContext) convertImage() error {

	config, err := common.GetSettingsConfig(common.SystemConfig)
	if err != nil {
		logging.Errorf("RestoreContext: Error from retrieving indexer settings. Error = %v", err)
		return err
	}

	var delTokens map[common.IndexDefnId]*mc.DeleteCommandToken
	delTokens, err = mc.FetchIndexDefnToDeleteCommandTokensMap()
	if err != nil {
		logging.Errorf("RestoreContext: Error in FetchIndexDefnToDeleteCommandTokensMap %v", err)
		return err
	}

	var buildTokens map[common.IndexDefnId]*mc.BuildCommandToken
	buildTokens, err = mc.FetchIndexDefnToBuildCommandTokensMap()
	if err != nil {
		logging.Errorf("RestoreContext: Error in FetchIndexDefnToBuildCommandTokensMap %v", err)
		return err
	}

	for _, metadata := range m.image.Metadata {

		meta := transformMeta(&metadata)

		for _, defn := range meta.IndexDefinitions {

			defn.SetCollectionDefaults()
			// If the index is in CREATED state, this will return nil.  So if there is any create index in flight,
			// it could be excluded by restore.
			indexes, err := planner.ConvertToIndexUsage(config, &defn, meta, buildTokens, delTokens)
			if err != nil {
				return err
			}

			if len(indexes) == 0 {
				logging.Infof("RestoreContext:  Index could be in the process of being created or dropped.  Skip restoring index (%v, %v, %v, %v).",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name)
				continue
			}

			for _, index := range indexes {

				if index.Instance != nil {
					logging.Infof("RestoreContext:  Processing index in backup image (%v, %v, %v, %v %v, %v).",
						index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.ReplicaId)

					m.defnInImage[index.Instance.Defn.DefnId] = true
				} else {
					logging.Infof("RestoreContext:  Processing index in backup image (%v, %v, %v, %v).",
						index.Bucket, index.Scope, index.Collection, index.Name)

					m.defnInImage[defn.DefnId] = true
				}

				m.idxFromImage[common.IndexerId(meta.IndexerId)] = append(m.idxFromImage[common.IndexerId(meta.IndexerId)], index)
			}
		}
	}

	return nil
}

//
// Remove duplicate index defintion from image.   Dupcliate index defintion include:
// 1) Index with RState=Pending
// 2) Index with different instance versions
//
func (m *RestoreContext) cleanseBackupMetadata() {

	for indexerId, indexes := range m.idxFromImage {
		newIndexes := ([]*planner.IndexUsage)(nil)

		for _, index := range indexes {

			if index.Instance == nil {
				logging.Infof("RestoreContext:  Skip restoring orphan index with no instance metadata (%v, %v, %v, %v, %v).",
					index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId)
				continue
			}

			// ignore any index with RState being pending.
			// **For pre-spock backup, RState of an instance is ACTIVE (0).
			if index.Instance != nil && index.Instance.RState != common.REBAL_ACTIVE {
				logging.Infof("RestoreContext:  Skip restoring RState PENDING index (%v, %v, %v, %v, %v).",
					index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId)
				continue
			}

			// find another instance with a higher instance version.
			// **For pre-spock backup, inst version is always 0. In fact, there should not be another instance (max == inst).
			max := findMaxVersionInst(m.idxFromImage, index.DefnId, index.PartnId, index.InstId)
			if max != index {
				logging.Infof("RestoreContext:  Skip restoring index (%v, %v, %v, %v, %v) with lower version number %v.",
					index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.Version)
				continue
			}

			newIndexes = append(newIndexes, index)
		}

		m.idxFromImage[indexerId] = newIndexes
	}
}

//
// Prepare a map of index name to any instance of that index in the current placement
//
func (m *RestoreContext) prepareInstNameMap() {
	for _, indexers := range m.current.Placement {
		for _, index := range indexers.Indexes {
			if index.Instance != nil {
				key := fmt.Sprintf("%v:%v:%v:%v", index.Bucket, index.Scope, index.Collection, index.Name)
				m.instNameMap[key] = index
			}
		}
	}
}

//
// Pick out the indexes that are not yet created in the existing cluster.
//
func (m *RestoreContext) findIndexToRestore() error {

	defnId2NameMap := make(map[common.IndexDefnId]string)
	replicaToRestore := make(map[common.IndexDefnId]map[int]common.IndexInstId)

	for indexerId, indexes := range m.idxFromImage {

		for _, index := range indexes {

			m.origBucket[index.Bucket] = true
			if len(m.target) != 0 && len(m.origBucket) > 1 {
				return fmt.Errorf("Backup metadata has indexes from multiple buckets.  Cannot restore indexes to a single bucket %v", m.target)
			}

			// Apply filters
			// For bucket level filter, don't filter anything if target bucket is not
			// specified. If target bucket is specified, then don't apply bucket level
			// filter.
			filtBucket := ""
			if len(m.target) != 0 {
				filtBucket = index.Bucket
			}

			if !applyFilters(filtBucket, index.Bucket, index.Scope, index.Collection, "", m.filters, m.filterType) {
				logging.Debugf("RestoreContext:  Skip restoring index (%v, %v, %v, %v) due to filters.",
					index.Bucket, index.Scope, index.Collection, index.Name)
				continue
			}

			// Remap the bucket, scope, collection if needed.
			err := m.remapIndex(index)
			if err != nil {
				return err
			}

			// ignore any index with RState being pending
			// **For pre-spock backup, RState of an instance is ACTIVE (0).
			if index.Instance == nil || index.Instance.RState != common.REBAL_ACTIVE {
				logging.Infof("RestoreContext:  Skip restoring RState PENDING index (%v, %v, %v, %v).",
					index.Bucket, index.Scope, index.Collection, index.Name)
				continue
			}

			// Find if the index already exist in the current cluster with matching name, bucket, scope and collection.
			anyInst := findMatchingInst(m.instNameMap, index.Bucket, index.Scope, index.Collection, index.Name)
			if anyInst != nil {

				// if there is matching index, check if it has the same definition.
				if common.IsEquivalentIndex(&anyInst.Instance.Defn, &index.Instance.Defn) {

					// if it has the same definiton, check if the same replica exist
					// ** For pre-spock backup, ReplicaId is 0
					// ** For pre-vulcan backup, PartnId is 0
					matchingReplica := findMatchingReplica(m.current, index.Bucket, index.Scope, index.Collection,
						index.Name, index.PartnId, index.Instance.ReplicaId)

					// If same replica exist, then skip.
					if matchingReplica != nil {
						logging.Infof("RestoreContext:  Find index in the target cluster with the same bucket, name, replicaId and definition. "+
							"Skip restoring index (%v, %v, %v, %v, %v, %v).", index.Bucket, index.Scope, index.Collection, index.Name,
							index.PartnId, index.Instance.ReplicaId)
						continue
					}

					// This replica on the partition does not exit, have we seen the same replica before?
					if _, ok := replicaToRestore[index.DefnId][index.Instance.ReplicaId]; !ok {
						// If same replica does not exist, restore only if the replicaId is smaller than the no. of replica.
						numReplica := findNumReplica(m.current.Placement, anyInst.DefnId) + len(replicaToRestore[index.DefnId])
						if int(anyInst.Instance.Defn.GetNumReplica()+1) <= int(numReplica) {
							logging.Infof("RestoreContext:  Find index in the target cluster with the same bucket, name and definition, but fewer replica. "+
								"Skip restoring index (%v, %v, %v, %v, %v, %v).", index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.ReplicaId)
							continue
						}

						// Check if there are enough number of indexer nodes to replica repair
						if numReplica >= len(m.current.Placement) {
							logging.Infof("RestoreContext:  There aren't enough number of indexer nodes to place the replica. "+
								"Skip restoring index (%v, %v,%v, %v, %v, %v).", index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.ReplicaId)
							continue
						}
					}
				} else {
					// There is another index with the same name or bucket but different definition.  Re-name the index to restore.
					newName := m.getNewName(&defnId2NameMap, index.Bucket, index.Scope,
						index.Collection, index.Name, index.DefnId)

					logging.Infof("RestoreContext:  Find index (with different defn) in the target cluster with the same bucket and index name .  "+
						" Renaming index from (%v, %v, %v, %v, %v) to (%v, %v, %v).",
						index.Bucket, index.Scope, index.Collection, index.Name, index.Instance.ReplicaId, index.Bucket, defnId2NameMap[index.DefnId], index.Instance.ReplicaId)

					index.Name = newName
					index.Instance.Defn.Name = newName
				}
			} else {

				// Note that there is an inherent race between index creation and
				// restore where cluster may end up having duplicate index name.
				// This race exists even without the presence of schedule create tokens.

				// There isn't any matching instance in the current metadata.
				// Check if there is any valid schedule create token with same index name.

				if token := findMatchingSchedToken(m.schedTokens, index.Bucket, index.Scope,
					index.Collection, index.Name); token != nil {

					// Check if it is an equivalent index.
					if common.IsEquivalentIndex(&token.Definition, &index.Instance.Defn) {
						// Prioritise the existing token over the index being restored, as
						// it was created before the restore was triggered.
						//
						// As existing token is not yet built (otherwise findMatchingInst
						// would have returned an instance), missing replica or partition
						// are irrelevant.

						logging.Infof("RestoreContext:  Find schedule create token in the target cluster with the same bucket, scope, collection, name. "+
							"Skip restoring index (%v, %v, %v, %v, %v, %v).", index.Bucket, index.Scope, index.Collection, index.Name,
							index.PartnId, index.Instance.ReplicaId)
						continue
					}

					// There is another index with the same name or bucket but different definition.  Re-name the index to restore.
					newName := m.getNewName(&defnId2NameMap, index.Bucket, index.Scope,
						index.Collection, index.Name, index.DefnId)

					logging.Infof("RestoreContext:  Find schedule create token (with different defn) in the target cluster with the same bucket and index name .  "+
						" Renaming index from (%v, %v, %v, %v, %v) to (%v, %v, %v).",
						index.Bucket, index.Scope, index.Collection, index.Name, index.Instance.ReplicaId, index.Bucket, defnId2NameMap[index.DefnId], index.Instance.ReplicaId)

					index.Name = newName
					index.Instance.Defn.Name = newName
				}
			}

			// For existing schedule create token (with same name), findAnyReplica will return nil
			// and hence new instId will be generated and used.

			// Find if there is a replica in the existing cluster with the same replicaId (regardless of partition)
			instId := common.IndexInstId(0)
			anyReplica := findAnyReplica(m.current, index.Bucket, index.Scope, index.Collection, index.Name, index.Instance.ReplicaId)
			if anyReplica != nil {
				// There is already a replica with the same replicaId, then use the same InstId
				instId = anyReplica.InstId

			} else {
				// This is a new replica to restore
				if _, ok := replicaToRestore[index.DefnId]; !ok {
					replicaToRestore[index.DefnId] = make(map[int]common.IndexInstId)
				}

				instId = replicaToRestore[index.DefnId][index.Instance.ReplicaId]
				if instId == common.IndexInstId(0) {
					var err error
					instId, err = common.NewIndexInstId()
					if err != nil {
						return err
					}
					replicaToRestore[index.DefnId][index.Instance.ReplicaId] = instId
				}
			}

			temp := *index
			temp.InstId = instId
			if temp.Instance != nil {
				temp.Instance.InstId = instId
			}
			m.idxToRestore[common.IndexerId(indexerId)] = append(m.idxToRestore[common.IndexerId(indexerId)], &temp)
		}
	}

	return nil
}

func (m *RestoreContext) findSchedTokensToRestore() error {

	// This function checks for existing indexes and existing schedule create
	// tokens in the target cluser. So, missing replicas and partitions are irrelevant.

	// TODO: defnId2NameMap isn't really required here
	defnId2NameMap := make(map[common.IndexDefnId]string)

	maxReplicas := len(m.current.Placement) - 1

	for defnId, token := range m.image.SchedTokens {
		logging.Infof("RestoreContext:  Processing schedule create token in backup image (%v, %v, %v, %v).",
			token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name)

		m.origBucket[token.Definition.Bucket] = true

		if len(m.target) != 0 && len(m.origBucket) > 1 {
			return fmt.Errorf("Backup metadata have indexes from multiple buckets.  Cannot restore indexes to a single bucket %v", m.target)
		}

		// Apply Filter
		// For bucket level filter, don't filter anything if target bucket is not
		// specified. If target bucket is specified, then don't apply bucket level
		// filter.
		filtBucket := ""
		if len(m.target) != 0 {
			filtBucket = token.Definition.Bucket
		}

		if !applyFilters(filtBucket, token.Definition.Bucket, token.Definition.Scope,
			token.Definition.Collection, "", m.filters, m.filterType) {

			logging.Debugf("RestoreContext:  Skip restoring index (%v, %v, %v, %v) due to filters.",
				token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name)
			continue
		}

		// Remap
		if err := m.remapToken(&token.Definition); err != nil {
			return err
		}

		// Update num replica as per cluster topology
		if token.Definition.GetNumReplica() >= len(m.current.Placement) {
			logging.Infof("RestoreContext: For index (%v, %v, %v, %v), setting num replica to %v, based on cluster topology.",
				token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection,
				token.Definition.Name, maxReplicas)

			// There aren't any other versions/instanced of token.Definition.
			// So just update the NumReplica.
			token.Definition.NumReplica = uint32(maxReplicas)
		}

		// Check for existing instance/token
		anyInst := findMatchingInst(m.instNameMap, token.Definition.Bucket, token.Definition.Scope,
			token.Definition.Collection, token.Definition.Name)
		if anyInst != nil {
			// if there is matching index, check if it has the same definition.
			if common.IsEquivalentIndex(&anyInst.Instance.Defn, &token.Definition) {
				logging.Infof("RestoreContext:  Find index in the target cluster with the same bucket, name and definition. "+
					"Skip restoring schedule create token (%v, %v, %v, %v).", token.Definition.Bucket, token.Definition.Scope,
					token.Definition.Collection, token.Definition.Name)
				continue
			}

			// There is another index with the same name or bucket but different definition.  Re-name the index to restore.
			newName := m.getNewName(&defnId2NameMap, token.Definition.Bucket, token.Definition.Scope,
				token.Definition.Collection, token.Definition.Name, token.Definition.DefnId)

			logging.Infof("RestoreContext:  Find schedule create token (with different defn) in the target cluster with the same bucket and index name .  "+
				" Renaming token from (%v, %v, %v, %v) to (%v, %v).",
				token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name, token.Definition.Bucket, newName)

			token.Definition.Name = newName
			m.tokToRestore[defnId] = token

		} else {

			if existToken := findMatchingSchedToken(m.schedTokens, token.Definition.Bucket, token.Definition.Scope,
				token.Definition.Collection, token.Definition.Name); existToken != nil {

				// Check if it is an equivalent index.
				if common.IsEquivalentIndex(&existToken.Definition, &token.Definition) {
					// Prioritise the existing token over the schedule create token
					// being restored.

					logging.Infof("RestoreContext:  Find schedule create token in the target cluster with the same bucket, scope, collection, name. "+
						"Skip restoring schedule create token (%v, %v, %v, %v).", token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection,
						token.Definition.Name)
					continue
				}

				// There is another index with the same name or bucket but different definition.  Re-name the index to restore.
				newName := m.getNewName(&defnId2NameMap, token.Definition.Bucket, token.Definition.Scope,
					token.Definition.Collection, token.Definition.Name, token.Definition.DefnId)

				logging.Infof("RestoreContext:  Find schedule create token (with different defn) in the target cluster with the same bucket and index name .  "+
					" Renaming token from (%v, %v, %v, %v) to (%v, %v).",
					token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name, token.Definition.Bucket, newName)

				token.Definition.Name = newName
				m.tokToRestore[defnId] = token
			} else {
				// Neither a matching instance nor a matching schedule create token exists
				m.tokToRestore[defnId] = token
			}

		}
	}

	// TODO: Check for number of replicas and check if there are enough number of indexer nodes

	return nil
}

//
// For each indexer in the image, try to find a correpsonding indexer.
//
func (m *RestoreContext) buildIndexerMapping() {

	// find a match for each indexer node in the image
	excludes := ([]*planner.IndexerNode)(nil)

	for indexerId, indexes := range m.idxFromImage {

		// This will not match any empty indexer node
		if match := findMatchingIndexer(indexerId, indexes, m.current.Placement, excludes); match != nil {

			logging.Infof("RestoreContext:  Map indexer %v in image to indexer %v in current cluster.", indexerId, match.IndexerId)

			m.indexerMap[common.IndexerId(indexerId)] = common.IndexerId(match.IndexerId)
			excludes = append(excludes, match)
		}
	}
}

//
// Update index definition id according to the indexes in current cluster.
//
func (m *RestoreContext) updateIndexDefnId() error {

	defnIdMap := make(map[string]common.IndexDefnId)

	// map indexes in existing cluster
	for _, indexer := range m.current.Placement {
		for _, index := range indexer.Indexes {
			key := fmt.Sprintf("%v %v %v %v", index.Bucket, index.Scope, index.Collection, index.Name)
			defnIdMap[key] = index.DefnId
		}
	}

	// map indexes that are to be restored.   Generate a new defnId if
	// the index has not been seen before.
	for _, indexes := range m.idxToRestore {
		for _, index := range indexes {
			key := fmt.Sprintf("%v %v %v %v", index.Bucket, index.Scope, index.Collection, index.Name)
			if _, ok := defnIdMap[key]; !ok {
				defnId, err := common.NewIndexDefnId()
				if err != nil {
					logging.Errorf("RestoreContext: fail to generate index definition id %v", err)
					return err
				}
				defnIdMap[key] = defnId
			}

			// Planner depends on 3 fields
			// 1) Index Defnition Id
			// 2) ReplicaId
			// 3) PartitionId
			// Regenerate IndexDefnId for all replica/partitions with the same <bucket,scope,collection,name>
			index.DefnId = defnIdMap[key]
			if index.Instance != nil {
				index.Instance.Defn.DefnId = defnIdMap[key]
			}
		}
	}

	newTokToRestore := make(map[common.IndexDefnId]*mc.ScheduleCreateToken)
	for _, token := range m.tokToRestore {
		newDefnId, err := common.NewIndexDefnId()
		if err != nil {
			logging.Errorf("RestoreContext: Cannot restore schedule create token for (%v, %v, %v, %v) due to error in NewIndexDefnId %v.",
				token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name, err)
			return err
		}

		token.Definition.DefnId = newDefnId
		newTokToRestore[newDefnId] = token
	}
	m.tokToRestore = newTokToRestore

	return nil
}

//
// Place index
//
func (m *RestoreContext) placeIndex() (map[string][]*common.IndexDefn, error) {

	newNodes := ([]*planner.IndexerNode)(nil)
	newNodeIds := ([]string)(nil)
	mappedIndexers := ([]*planner.IndexerNode)(nil)

	// Go through the index that needs to be restored
	for indexerId, indexes := range m.idxToRestore {

		// Found a matching indexer in the target cluster
		if targetIndexerId, ok := m.indexerMap[indexerId]; ok {
			if indexer := findIndexer(m.current, targetIndexerId); indexer != nil {
				addIndexes(indexes, indexer)
				mappedIndexers = append(mappedIndexers, indexer)
				continue
			}
		}

		// cannot find a matching indexer in the target.  Add a new node
		// to the current plan
		indexer := createNewEjectedNode(indexes, indexerId)
		newNodes = append(newNodes, indexer)
		newNodeIds = append(newNodeIds, indexer.NodeId)
	}

	// Restore the schedule create tokens as indexes. This ensures simplicity
	// of the manual steps required after restore as lifecycle of schedule
	// create tokens is different from regular indexes being restored.

	// Create a new ejected node if required and if none exists.
	newNodeForced := false
	if len(newNodes) == 0 && len(m.tokToRestore) != 0 {
		newIndexerId, err := common.NewUUID()
		if err != nil {
			return nil, err
		}

		newNodeForced = true
		indexer := createNewEjectedNode(nil, common.IndexerId(newIndexerId.Str()))
		newNodes = append(newNodes, indexer)
		newNodeIds = append(newNodeIds, indexer.NodeId)
		logging.Infof("RestoreContext: Created new ejected node %v for schedule create tokens", indexer)
	}

	// Place all indexes from schedule tokens on ejected nodes with
	// round robin.
	i := 0
	var sizing planner.SizingMethod
	tokIndexes := make([]*planner.IndexUsage, 0, len(m.tokToRestore))
	for _, token := range m.tokToRestore {
		spec := prepareIndexSpec(&token.Definition)
		sizing = planner.GetNewGeneralSizingMethod()
		idxUsages, err := planner.IndexUsagesFromSpec(sizing, []*planner.IndexSpec{spec})
		if err != nil {
			return nil, err
		}

		if !newNodeForced {
			newNodes[i].AddIndexes(idxUsages)
			i = (i + 1) % len(newNodes)
		} else {
			tokIndexes = append(tokIndexes, idxUsages...)
		}
	}

	if newNodeForced {
		newNodes[0].SetIndexes(tokIndexes)
	}

	// add new nodes to current plan
	for _, indexer := range newNodes {
		m.current.Placement = append(m.current.Placement, indexer)

		// ComputeSizing is an idempotent operation.
		indexer.ComputeSizing(sizing)
	}

	logging.Infof("RestoreContext:  Index Layout before planning")
	for _, indexer := range m.current.Placement {
		logging.Infof("\tRestoreContext:  Indexer NodeId %v IndexerId %v", indexer.NodeId, indexer.IndexerId)
		for _, index := range indexer.Indexes {
			if index.Instance != nil {
				logging.Infof("\t\tRestoreContext:  Index (%v, %v, %v, %v, %v)", index.Bucket, index.Scope,
					index.Collection, index.Name, index.Instance.ReplicaId)
			} else {
				logging.Infof("\t\tRestoreContext:  Index (%v, %v, %v, %v)", index.Bucket, index.Scope,
					index.Collection, index.Name)
			}
		}
	}

	// If there is enough empty nodes in the current clsuter to do a simple swap rebalance.
	numEmptyIndexer := findNumEmptyIndexer(m.current.Placement, mappedIndexers)
	if numEmptyIndexer >= len(newNodes) {
		// place indexes using swap rebalance
		solution, err := planner.ExecuteSwapWithOptions(m.current, true, "", "", 0, -1, -1, false, newNodeIds)
		if err == nil {
			return m.buildIndexHostMapping(solution), nil
		}
	}

	// place indexes using regular rebalance
	solution, err := planner.ExecuteRebalanceWithOptions(m.current, nil, true, "", "", 0, -1, -1, false, newNodeIds)
	if err == nil {
		return m.buildIndexHostMapping(solution), nil
	}

	if err != nil {
		logging.Errorf("RestoreContext:  Fail to plan index during restore.  Error = %v", err)
	}

	return nil, err
}

//
// Find new home for the index
//
func (m *RestoreContext) buildIndexHostMapping(solution *planner.Solution) map[string][]*common.IndexDefn {

	result := make(map[string][]*common.IndexDefn)

	for _, indexes := range m.idxToRestore {
		for _, index := range indexes {
			if index.Instance != nil {
				if indexer := solution.FindIndexerWithReplica(index.Name, index.Bucket, index.Scope, index.Collection,
					index.PartnId, index.Instance.ReplicaId); indexer != nil {

					logging.Infof("RestoreContext:  Restoring index (%v, %v, %v, %v, %v, %v) at indexer %v",
						index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.ReplicaId, indexer.NodeId)

					defns := result[indexer.RestUrl]
					found := false
					for _, defn := range defns {
						if defn.DefnId == index.Instance.Defn.DefnId && defn.ReplicaId == index.Instance.ReplicaId {
							found = true
							defn.Partitions = append(defn.Partitions, index.PartnId)
							defn.Versions = append(defn.Versions, 0)
							break
						}
					}

					if !found {
						if index.Instance != nil {
							// Reset bucketUUID since it would have changed
							index.Instance.Defn.BucketUUID = ""

							// Reset Scope and Collection Ids
							index.Instance.Defn.ScopeId = ""
							index.Instance.Defn.CollectionId = ""

							// update transient fields
							index.Instance.Defn.InstId = index.Instance.InstId
							index.Instance.Defn.ReplicaId = index.Instance.ReplicaId
							index.Instance.Defn.Partitions = []common.PartitionId{index.PartnId}
							index.Instance.Defn.Versions = []int{0}
							index.Instance.Defn.InstVersion = 0
							index.Instance.Defn.NumPartitions = uint32(index.Instance.Pc.GetNumPartitions())

							result[indexer.RestUrl] = append(result[indexer.RestUrl], &index.Instance.Defn)
						} else {
							logging.Errorf("RestoreContext:  Cannot restore index (%v, %v, %v, %v, %v, %v) at indexer %v because index instance is missing.",
								index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, index.Instance.ReplicaId, indexer.NodeId)
						}
					}
				}
			}
		}
	}

	// Find new home for all the replicas and partitions of the schedule create tokens.
	defnMap := make(map[common.IndexDefnId]map[string]*common.IndexDefn)
	instIdMap := make(map[common.IndexDefnId]map[int]common.IndexInstId)
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if token, ok := m.tokToRestore[index.DefnId]; ok {
				if index.Instance == nil {
					logging.Errorf("RestoreContext: Cannot restore schedule create token for (%v, %v, %v, %v, %v) at indexer %v as index instance is missinig.",
						token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name, index.PartnId, indexer)
					continue
				}

				var indexerMap map[string]*common.IndexDefn
				var ok bool
				if indexerMap, ok = defnMap[index.DefnId]; !ok {
					indexerMap = make(map[string]*common.IndexDefn)
					defnMap[index.DefnId] = indexerMap
				}

				if defn, ok := indexerMap[indexer.RestUrl]; !ok {
					if _, ok := instIdMap[index.DefnId]; !ok {
						instIdMap[index.DefnId] = make(map[int]common.IndexInstId)
					}

					instId, ok := instIdMap[index.DefnId][index.Instance.ReplicaId]
					if !ok {
						var err error

						instId, err = common.NewIndexInstId()
						if err != nil {
							logging.Errorf("RestoreContext: Cannot restore schedule create token for (%v, %v, %v, %v, %v) at indexer %v due to error in NewIndexInstId %v.",
								token.Definition.Bucket, token.Definition.Scope, token.Definition.Collection, token.Definition.Name, index.PartnId, indexer, err)
							continue
						}

						instIdMap[index.DefnId][index.Instance.ReplicaId] = instId
					}

					defn := &token.Definition
					temp := *defn
					temp.InstId = instId
					temp.ReplicaId = index.Instance.ReplicaId
					temp.Partitions = []common.PartitionId{index.PartnId}
					temp.Versions = []int{0}
					indexerMap[indexer.RestUrl] = &temp
					result[indexer.RestUrl] = append(result[indexer.RestUrl], &temp)
				} else {
					defn.Partitions = append(defn.Partitions, index.PartnId)
					defn.Versions = append(defn.Versions, 0)
				}
			}
		}
	}

	return result
}

func (m *RestoreContext) remapIndex(index *planner.IndexUsage) error {

	// Remap bucket, if required
	if len(m.target) != 0 && index.Bucket != m.target {
		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket (%v)",
			index.Name, index.Bucket, index.Scope, index.Collection, m.target)
		index.Bucket = m.target
		if index.Instance != nil {
			index.Instance.Defn.Bucket = m.target
		}
	}

	// Input validation ensures non-overlapping scope and collection remaps
	// Remap scope, if required
	if newScope, ok := m.remap[index.Scope]; ok {
		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket, scope (%v, %v)", index.Name, index.Bucket, index.Scope, index.Collection,
			m.target, newScope)

		index.Scope = newScope
		if index.Instance != nil {
			index.Instance.Defn.Scope = newScope
		}
	}

	// Remap collection, if required
	if newColl, ok := m.remap[fmt.Sprintf("%v.%v", index.Scope, index.Collection)]; ok {
		sc := strings.Split(newColl, ".")
		if len(sc) != 2 {
			err := fmt.Errorf("Error in restoring index %v:%v:%v:%v due to malformed remap",
				index.Bucket, index.Scope, index.Collection, index.Name)
			logging.Errorf(err.Error())
			return err
		}

		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket, scope, collection (%v, %v, %v)", index.Name, index.Bucket, index.Scope, index.Collection,
			m.target, sc[0], sc[1])

		index.Scope = sc[0]
		index.Collection = sc[1]
		if index.Instance != nil {
			index.Instance.Defn.Scope = sc[0]
			index.Instance.Defn.Collection = sc[1]
		}
	}

	return nil
}

// TODO: Avoid code duplication across remapIndex and remapToken.
// Note that the tokens won't have instance.
func (m *RestoreContext) remapToken(index *common.IndexDefn) error {

	// Remap bucket, if required
	if len(m.target) != 0 && index.Bucket != m.target {
		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket (%v)",
			index.Name, index.Bucket, index.Scope, index.Collection, m.target)
		index.Bucket = m.target
	}

	// Input validation ensures non-overlapping scope and collection remaps
	// Remap scope, if required
	if newScope, ok := m.remap[index.Scope]; ok {
		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket, scope (%v, %v)", index.Name, index.Bucket, index.Scope, index.Collection,
			m.target, newScope)

		index.Scope = newScope
	}

	// Remap collection, if required
	if newColl, ok := m.remap[fmt.Sprintf("%v.%v", index.Scope, index.Collection)]; ok {
		sc := strings.Split(newColl, ".")
		if len(sc) != 2 {
			err := fmt.Errorf("Error in restoring index %v:%v:%v:%v due to malformed remap",
				index.Bucket, index.Scope, index.Collection, index.Name)
			logging.Errorf(err.Error())
			return err
		}

		logging.Infof("RestoreContext:  convert index %v from bucket %v scope %v collection %v "+
			"to target bucket, scope, collection (%v, %v, %v)", index.Name, index.Bucket, index.Scope, index.Collection,
			m.target, sc[0], sc[1])

		index.Scope = sc[0]
		index.Collection = sc[1]
	}

	return nil
}

//
// Get new name for the index
//
func (m *RestoreContext) getNewName(defnId2NameMap *map[common.IndexDefnId]string,
	bucket, scope, collection, name string, defnId common.IndexDefnId) string {

	if _, ok := (*defnId2NameMap)[defnId]; !ok {
		for count := 0; true; count++ {
			newName := fmt.Sprintf("%v_%v", name, count)
			if findMatchingInst(m.instNameMap, bucket, scope, collection, newName) != nil {
				continue
			}

			if findMatchingSchedToken(m.schedTokens, bucket, scope, collection, newName) != nil {
				continue
			}

			(*defnId2NameMap)[defnId] = newName
			break
		}
	}

	return (*defnId2NameMap)[defnId]
}

//////////////////////////////////////////////////////////////
// Utility
//////////////////////////////////////////////////////////////

//
// Find a higest version index instance with the same definition id and instance id
//
func findMaxVersionInst(metadata map[common.IndexerId][]*planner.IndexUsage, defnId common.IndexDefnId, partnId common.PartitionId,
	instId common.IndexInstId) *planner.IndexUsage {

	max := (*planner.IndexUsage)(nil)

	for _, indexes := range metadata {
		for _, index := range indexes {

			// ignore any index with RState being pending
			if index.Instance == nil || index.Instance.RState != common.REBAL_ACTIVE {
				continue
			}

			if index.DefnId == defnId && index.PartnId == partnId && index.InstId == instId {
				if max == nil || index.Instance.Version > max.Instance.Version {
					max = index
				}
			}
		}
	}

	return max
}

//
// Find any instance in the metadata, regardless of its partitionId, version or RState
//
func findMatchingInst(instNameMap map[string]*planner.IndexUsage, bucket, scope, collection, name string) *planner.IndexUsage {

	key := fmt.Sprintf("%v:%v:%v:%v", bucket, scope, collection, name)
	if inst, ok := instNameMap[key]; ok {
		return inst
	}

	return nil
}

//
// Find matching replica in the metadata, regardless of its version or RState
//
func findMatchingReplica(current *planner.Plan, bucket, scope, collection, name string, partnId common.PartitionId, replicaId int) *planner.IndexUsage {

	for _, indexers := range current.Placement {
		for _, index := range indexers.Indexes {
			if index.Instance != nil {
				if index.Bucket == bucket &&
					index.Scope == scope &&
					index.Collection == collection &&
					index.Name == name &&
					index.PartnId == partnId &&
					index.Instance.ReplicaId == replicaId {

					return index
				}
			}
		}
	}

	return nil
}

//
// Find any replica in the metadata, regardless of its partitionId, version or RState
//
func findAnyReplica(current *planner.Plan, bucket, scope, collection, name string, replicaId int) *planner.IndexUsage {

	for _, indexers := range current.Placement {
		for _, index := range indexers.Indexes {
			if index.Instance != nil {
				if index.Bucket == bucket &&
					index.Scope == scope &&
					index.Collection == collection &&
					index.Name == name &&
					index.Instance.ReplicaId == replicaId {

					return index
				}
			}
		}
	}

	return nil
}

//
// Find a node in candidates which is a subset of the given indexes.  This is to honor
// original layout even if the restore operation has to restart in a cluster that is
// partially restored.
//
func findMatchingIndexer(indexerId common.IndexerId, indexes []*planner.IndexUsage,
	candidates []*planner.IndexerNode, excludes []*planner.IndexerNode) *planner.IndexerNode {

	// nothing to match
	if len(indexes) == 0 {
		return nil
	}

	for _, indexer := range candidates {

		// should we consider this target?
		if isMember(excludes, indexer) {
			continue
		}

		// If indexerId match
		if indexerId == common.IndexerId(indexer.IndexerId) {
			return indexer
		}

		// is target a subset of source?
		if len(indexer.Indexes) != 0 && isSubset(indexes, indexer.Indexes) {
			return indexer
		}
	}

	return nil
}

//
// Find the number of empty indexer nodes
//
func findNumEmptyIndexer(indexers []*planner.IndexerNode, excludes []*planner.IndexerNode) int {

	count := 0
	for _, indexer := range indexers {

		// should we consider this target?
		if isMember(excludes, indexer) {
			continue
		}

		if len(indexer.Indexes) == 0 {
			count++
		}
	}

	return count
}

//
// Find if there is a match in the candidate list
//
func isMember(candidates []*planner.IndexerNode, indexer *planner.IndexerNode) bool {

	for _, candidate := range candidates {
		if candidate == indexer {
			return true
		}
	}

	return false
}

//
// Is one a subset of another?
//
func isSubset(superset []*planner.IndexUsage, subset []*planner.IndexUsage) bool {

	for _, sub := range subset {
		found := false
		for _, super := range superset {
			if super.Bucket == sub.Bucket &&
				super.Scope == sub.Scope &&
				super.Collection == sub.Collection &&
				super.Name == sub.Name &&
				super.PartnId == sub.PartnId &&
				super.Instance != nil && sub.Instance != nil && super.Instance.ReplicaId == sub.Instance.ReplicaId {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

//
// Find the indexer node given the indexerId
//
func findIndexer(plan *planner.Plan, indexerId common.IndexerId) *planner.IndexerNode {

	for _, indexer := range plan.Placement {
		if common.IndexerId(indexer.IndexerId) == indexerId {
			return indexer
		}
	}

	return nil

}

//
// Create a new "ejected" indexer node to hold the indexes to restore
//
func createNewEjectedNode(indexes []*planner.IndexUsage, indexerId common.IndexerId) *planner.IndexerNode {

	nodeId := fmt.Sprintf("%v", indexerId)
	node := planner.CreateIndexerNodeWithIndexes(nodeId, nil, indexes)
	node.IndexerId = string(indexerId)
	node.MarkDeleted()

	return node
}

//
// Add Indexes to the indexer node
//
func addIndexes(indexes []*planner.IndexUsage, indexer *planner.IndexerNode) {

	for _, index := range indexes {
		indexer.Indexes = append(indexer.Indexes, index)
	}
}

//
// Find the number of replica across partitions for an index (including itself).
//
func findNumReplica(indexers []*planner.IndexerNode, defnId common.IndexDefnId) int {

	replicaMap := make(map[int]bool)
	for _, indexer := range indexers {
		for _, index := range indexer.Indexes {
			if defnId == index.DefnId {
				replicaMap[index.Instance.ReplicaId] = true
			}
		}
	}

	return len(replicaMap)
}

//
// Find valid matching schedule create token
//
func findMatchingSchedToken(tokens map[common.IndexDefnId]*mc.ScheduleCreateToken,
	bucket, scope, collection, name string) *mc.ScheduleCreateToken {

	for _, token := range tokens {
		if token.Definition.Bucket == bucket &&
			token.Definition.Scope == scope &&
			token.Definition.Collection == collection &&
			token.Definition.Name == name {

			return token
		}
	}

	return nil
}

//
// Prepare the index specs
//
func prepareIndexSpec(defn *common.IndexDefn) *planner.IndexSpec {

	var spec planner.IndexSpec
	spec.DefnId = defn.DefnId
	spec.Name = defn.Name
	spec.Bucket = defn.Bucket
	spec.Scope = defn.Scope
	spec.Collection = defn.Collection
	spec.IsPrimary = defn.IsPrimary
	spec.SecExprs = defn.SecExprs
	spec.WhereExpr = defn.WhereExpr
	spec.Deferred = defn.Deferred
	spec.Immutable = defn.Immutable
	spec.IsArrayIndex = defn.IsArrayIndex
	spec.Desc = defn.Desc
	spec.IndexMissingLeadingKey = defn.IndexMissingLeadingKey
	spec.NumPartition = uint64(defn.NumPartitions)
	spec.PartitionScheme = string(defn.PartitionScheme)
	spec.HashScheme = uint64(defn.HashScheme)
	spec.PartitionKeys = defn.PartitionKeys
	spec.Replica = uint64(defn.NumReplica) + 1
	spec.RetainDeletedXATTR = defn.RetainDeletedXATTR
	spec.ExprType = string(defn.ExprType)

	spec.NumDoc = defn.NumDoc
	spec.DocKeySize = defn.DocKeySize
	spec.SecKeySize = defn.SecKeySize
	spec.ArrKeySize = defn.SecKeySize
	spec.ArrSize = defn.ArrSize
	spec.ResidentRatio = defn.ResidentRatio
	spec.MutationRate = 0
	spec.ScanRate = 0

	// TODO: Set storage mode correcly.

	return &spec
}

//
// Copy metadata from type LocalIndexMetadata to planner.LocalIndexMetadata
//
func transformMeta(metadata *LocalIndexMetadata) *planner.LocalIndexMetadata {
	meta := new(planner.LocalIndexMetadata)
	meta.IndexerId = metadata.IndexerId
	meta.NodeUUID = metadata.NodeUUID
	meta.StorageMode = metadata.StorageMode
	meta.LocalSettings = metadata.LocalSettings
	meta.IndexDefinitions = metadata.IndexDefinitions

	meta.IndexTopologies = make([]mc.IndexTopology, 0)
	for _, topology := range metadata.IndexTopologies {
		meta.IndexTopologies = append(meta.IndexTopologies, *transformTopology(&topology))
	}

	return meta
}
