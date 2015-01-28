// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	"net"
	"runtime/debug"
)

//ClustMgrAgent provides the mechanism to talk to Index Coordinator
type ClustMgrAgent interface {
}

type clustMgrAgent struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	mgr    *manager.IndexManager //handle to index manager
	config common.Config

	metaNotifier manager.MetadataNotifier
}

func NewClustMgrAgent(supvCmdch MsgChannel, supvRespch MsgChannel, cfg common.Config) (
	ClustMgrAgent, Message) {

	//Init the clustMgrAgent struct
	c := &clustMgrAgent{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		config:     cfg,
	}

	var cinfo *common.ClusterInfoCache
	url, err := common.ClusterAuthUrl(cfg["clusterAddr"].String())
	if err == nil {
		cinfo, err = common.NewClusterInfoCache(url, DEFAULT_POOL)
	}
	if err != nil {
		common.Errorf("ClustMgrAgent::Fail to init ClusterInfoCache : %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		common.Errorf("ClustMgrAgent::Fail to init ClusterInfoCache : %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	node := cinfo.GetCurrentNode()
	scan_addr, err := cinfo.GetServiceAddress(node, "indexScan")
	if err != nil {
		common.Errorf("ClustMgrAgent::Fail to indexer scan address : %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	admin_addr, err := cinfo.GetServiceAddress(node, "indexAdmin")
	if err != nil {
		common.Errorf("ClustMgrAgent::Fail to indexer admin address : %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	mgr, err := manager.NewIndexManager(admin_addr, scan_addr, cfg)
	if err != nil {
		common.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}

	}

	c.mgr = mgr

	metaNotifier := NewMetaNotifier(supvRespch, cfg)
	if metaNotifier == nil {
		common.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR}}

	}

	mgr.RegisterNotifier(metaNotifier)

	c.metaNotifier = metaNotifier

	//start clustMgrAgent loop which listens to commands from its supervisor
	go c.run()

	//register with Index Manager for notification of metadata updates

	return c, &MsgSuccess{}

}

//run starts the clustmgrAgent loop which listens to messages
//from it supervisor(indexer)
func (c *clustMgrAgent) run() {

	//main ClustMgrAgent loop

	defer c.mgr.Close()

	defer c.panicHandler()

loop:
	for {
		select {

		case cmd, ok := <-c.supvCmdch:
			if ok {
				if cmd.GetMsgType() == CLUST_MGR_AGENT_SHUTDOWN {
					common.Infof("ClusterMgrAgent: Shutting Down")
					c.supvCmdch <- &MsgSuccess{}
					break loop
				}
				c.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (c *clustMgrAgent) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX:
		c.handleUpdateTopologyForIndex(cmd)

	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		c.handleGetGlobalTopology(cmd)

	case CLUST_MGR_GET_LOCAL:
		c.handleGetLocalValue(cmd)

	case CLUST_MGR_SET_LOCAL:
		c.handleSetLocalValue(cmd)

	default:
		common.Errorf("ClusterMgrAgent::handleSupvervisorCommands Unknown Message %v", cmd)
	}

}

func (c *clustMgrAgent) handleUpdateTopologyForIndex(cmd Message) {

	common.Debugf("ClustMgr:handleUpdateTopologyForIndex %v", cmd)

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()
	updatedFields := cmd.(*MsgClustMgrUpdate).GetUpdatedFields()

	updatedState := common.INDEX_STATE_NIL
	updatedStream := common.NIL_STREAM
	updatedError := ""

	for _, index := range indexList {
		if updatedFields.state {
			updatedState = index.State
		}
		if updatedFields.stream {
			updatedStream = index.Stream
		}
		if updatedFields.err {
			updatedError = index.Error
		}

		err := c.mgr.UpdateIndexInstance(index.Defn.Bucket, index.Defn.DefnId,
			updatedState, updatedStream, updatedError)
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}

}

func (c *clustMgrAgent) handleGetGlobalTopology(cmd Message) {

	common.Debugf("ClustMgr:handleGetGlobalTopology %v", cmd)

	//get the latest topology from manager
	metaIter, err := c.mgr.NewIndexDefnIterator()
	if err != nil {
		common.CrashOnError(err)
	}

	indexInstMap := make(common.IndexInstMap)

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		var idxDefn common.IndexDefn
		idxDefn = *defn

		t, e := c.mgr.GetTopologyByBucket(idxDefn.Bucket)
		if e != nil {
			common.CrashOnError(e)
		}

		inst := t.GetIndexInstByDefn(idxDefn.DefnId)

		if inst == nil {
			common.Warnf("ClustMgr:handleGetGlobalTopology Index Instance Not "+
				"Found For Index Definition %v. Ignored.", idxDefn)
			continue
		}

		//for indexer, Ready state doesn't matter. Till index build,
		//the index stays in Created state.
		var state common.IndexState
		instState := common.IndexState(inst.State)
		if instState == common.INDEX_STATE_READY {
			state = common.INDEX_STATE_CREATED
		} else {
			state = instState
		}

		idxInst := common.IndexInst{InstId: common.IndexInstId(inst.InstId),
			Defn:   idxDefn,
			State:  state,
			Stream: common.StreamId(inst.StreamId),
		}

		indexInstMap[idxInst.InstId] = idxInst

	}

	c.supvCmdch <- &MsgClustMgrTopology{indexInstMap: indexInstMap}
}

func (c *clustMgrAgent) handleGetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	common.Debugf("ClustMgr:handleGetLocalValue Key %v", key)

	val, err := c.mgr.GetLocalValue(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

func (c *clustMgrAgent) handleSetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()
	val := cmd.(*MsgClustMgrLocal).GetValue()

	common.Debugf("ClustMgr:handleSetLocalValue Key %v Value %v", key, val)

	err := c.mgr.SetLocalValue(key, val)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

//panicHandler handles the panic from index manager
func (c *clustMgrAgent) panicHandler() {

	//panic recovery
	if rc := recover(); rc != nil {
		var err error
		switch x := rc.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("Unknown panic")
		}

		common.Fatalf("ClusterMgrAgent Panic Err %v", err)
		common.StackTrace(string(debug.Stack()))

		//panic, propagate to supervisor
		msg := &MsgError{
			err: Error{code: ERROR_INDEX_MANAGER_PANIC,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
		c.supvRespch <- msg
	}

}

type metaNotifier struct {
	adminCh MsgChannel
	config  common.Config
}

func NewMetaNotifier(adminCh MsgChannel, config common.Config) *metaNotifier {

	if adminCh == nil {
		return nil
	}

	return &metaNotifier{
		adminCh: adminCh,
		config:  config,
	}

}

func (meta *metaNotifier) OnIndexCreate(indexDefn *common.IndexDefn) error {

	common.Debugf("clustMgrAgent::OnIndexCreate Notification "+
		"Received for Create Index %v", indexDefn)

	pc := meta.makeDefaultPartitionContainer()

	idxInst := common.IndexInst{InstId: common.IndexInstId(indexDefn.DefnId),
		Defn:  *indexDefn,
		State: common.INDEX_STATE_CREATED,
		Pc:    pc,
	}

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgCreateIndex{mType: CLUST_MGR_CREATE_INDEX_DDL,
		indexInst: idxInst,
		respCh:    respCh}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			common.Debugf("clustMgrAgent::OnIndexCreate Success "+
				"for Create Index %v", indexDefn)
			return nil

		case MSG_ERROR:
			common.Debugf("clustMgrAgent::OnIndexCreate Error "+
				"for Create Index %v. Error %v.", indexDefn, res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			common.Fatalf("clustMgrAgent::OnIndexCreate Unknown Response "+
				"Received for Create Index %v. Response %v", indexDefn, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		common.Debugf("clustMgrAgent::OnIndexCreate Unexpected Channel Close "+
			"for Create Index %v", indexDefn)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}
func (meta *metaNotifier) OnIndexBuild(indexDefnList []common.IndexDefnId) error {

	common.Debugf("clustMgrAgent::OnIndexBuild Notification "+
		"Received for Build Index %v", indexDefnList)

	respCh := make(MsgChannel)

	var indexInstList []common.IndexInstId
	for _, defnId := range indexDefnList {
		indexInstList = append(indexInstList, common.IndexInstId(defnId))
	}

	meta.adminCh <- &MsgBuildIndex{indexInstList: indexInstList,
		respCh: respCh}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			common.Debugf("clustMgrAgent::OnIndexBuild Success "+
				"for Build Index %v", indexDefnList)
			return nil

		case MSG_ERROR:
			common.Debugf("clustMgrAgent::OnIndexBuild Error "+
				"for Build Index %v. Error %v.", indexDefnList, res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			common.Fatalf("clustMgrAgent::OnIndexBuild Unknown Response "+
				"Received for Build Index %v. Response %v", indexDefnList, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		common.Debugf("clustMgrAgent::OnIndexBuild Unexpected Channel Close "+
			"for Create Index %v", indexDefnList)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnIndexDelete(defnId common.IndexDefnId) error {

	common.Debugf("clustMgrAgent::OnIndexDelete Notification "+
		"Received for Drop IndexId %v", defnId)

	respCh := make(MsgChannel)

	//Treat DefnId as InstId for now
	meta.adminCh <- &MsgDropIndex{mType: CLUST_MGR_DROP_INDEX_DDL,
		indexInstId: common.IndexInstId(defnId),
		respCh:      respCh}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			common.Debugf("clustMgrAgent::OnIndexDelete Success "+
				"for Drop IndexId %v", defnId)
			return nil

		case MSG_ERROR:
			common.Debugf("clustMgrAgent::OnIndexDelete Error "+
				"for Drop IndexId %v. Error %v", defnId, res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			common.Fatalf("clustMgrAgent::OnIndexDelete Unknown Response "+
				"Received for Drop IndexId %v. Response %v", defnId, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		common.Debugf("clustMgrAgent::OnIndexDelete Unexpected Channel Close "+
			"for Drop IndexId %v", defnId)
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (meta *metaNotifier) makeDefaultPartitionContainer() common.PartitionContainer {

	pc := common.NewKeyPartitionContainer()

	//Add one partition for now
	addr := net.JoinHostPort("", meta.config["streamMaintPort"].String())
	endpt := []common.Endpoint{common.Endpoint(addr)}

	partnDefn := common.KeyPartitionDefn{Id: common.PartitionId(1),
		Endpts: endpt}
	pc.AddPartition(common.PartitionId(1), partnDefn)

	return pc

}
