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

	mgr, err := manager.NewIndexManager(admin_addr, scan_addr)
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

	case CLUST_MGR_UPDATE_STATE_FOR_INDEX:
		c.handleUpdateStateForIndex(cmd)

	case CLUST_MGR_UPDATE_STREAM_FOR_INDEX:
		c.handleUpdateStreamForIndex(cmd)

	case CLUST_MGR_UPDATE_ERROR_FOR_INDEX:
		c.handleUpdateErrorForIndex(cmd)

	default:
		common.Errorf("ClusterMgrAgent::handleSupvervisorCommands Unknown Message %v", cmd)
	}

}

func (c *clustMgrAgent) handleUpdateStateForIndex(cmd Message) {

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()

	var ok bool
	var err error
	var t *manager.IndexTopology

	topologyMap := make(map[string]*manager.IndexTopology)
	for _, index := range indexList {
		if t, ok = topologyMap[index.Defn.Bucket]; !ok {
			t, err = c.mgr.GetTopologyByBucket(index.Defn.Bucket)
			if err != nil {
				common.CrashOnError(err)
			}
		}
		t.UpdateStateForIndexInstByDefn(common.IndexDefnId(index.InstId), index.State)
	}

	//set the topology back
	//TODO Check with John what happens if the topology has changed
	//after Get but before Set
	for b, t := range topologyMap {
		err = c.mgr.SetTopologyByBucket(b, t)
		if err != nil {
			common.CrashOnError(err)
		}
	}

}

func (c *clustMgrAgent) handleUpdateStreamForIndex(cmd Message) {

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()

	var ok bool
	var err error
	var t *manager.IndexTopology

	topologyMap := make(map[string]*manager.IndexTopology)
	for _, index := range indexList {
		if t, ok = topologyMap[index.Defn.Bucket]; !ok {
			t, err = c.mgr.GetTopologyByBucket(index.Defn.Bucket)
			if err != nil {
				common.CrashOnError(err)
			}
		}
		t.UpdateStateForIndexInstByDefn(common.IndexDefnId(index.InstId), index.State)
	}

	//set the topology back
	//TODO Check with John what happens if the topology has changed
	//after Get but before Set
	for b, t := range topologyMap {
		err = c.mgr.SetTopologyByBucket(b, t)
		if err != nil {
			common.CrashOnError(err)
		}
	}

}

func (c *clustMgrAgent) handleUpdateErrorForIndex(cmd Message) {

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()
	errStr := cmd.(*MsgClustMgrUpdate).GetErrorStr()

	var ok bool
	var err error
	var t *manager.IndexTopology

	topologyMap := make(map[string]*manager.IndexTopology)
	for _, index := range indexList {
		if t, ok = topologyMap[index.Defn.Bucket]; !ok {
			t, err = c.mgr.GetTopologyByBucket(index.Defn.Bucket)
			if err != nil {
				common.CrashOnError(err)
			}
		}
		t.SetErrorForIndexInstByDefn(common.IndexDefnId(index.InstId), errStr)
	}

	//set the topology back
	//TODO Check with John what happens if the topology has changed
	//after Get but before Set
	for b, t := range topologyMap {
		err = c.mgr.SetTopologyByBucket(b, t)
		if err != nil {
			common.CrashOnError(err)
		}
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

	pc := common.NewKeyPartitionContainer()

	//Add one partition for now
	addr := net.JoinHostPort("", meta.config["streamMaintPort"].String())
	endpt := []common.Endpoint{common.Endpoint(addr)}

	partnDefn := common.KeyPartitionDefn{Id: common.PartitionId(1),
		Endpts: endpt}
	pc.AddPartition(common.PartitionId(1), partnDefn)

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

func (meta *metaNotifier) OnTopologyUpdate(t *manager.IndexTopology) error {
	//No action required for topology updates for now
	return nil
}
