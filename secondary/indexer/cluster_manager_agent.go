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
	"strconv"
)

//ClustMgrAgent provides the mechanism to talk to Index Coordinator
type ClustMgrAgent interface {
}

type clustMgrAgent struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	mgr *manager.IndexManager //handle to index manager

	createNotifyCh <-chan interface{} //listen to create ddl from manager
	dropNotifyCh   <-chan interface{} //listen to drop ddl from manager
	config         common.Config
}

func NewClustMgrAgent(supvCmdch MsgChannel, supvRespch MsgChannel, cfg common.Config) (
	ClustMgrAgent, Message) {

	//Init the clustMgrAgent struct
	c := &clustMgrAgent{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		config:     cfg,
	}

	cinfo, err := common.NewClusterInfoCache(cfg["clusterAddr"].String(), DEFAULT_POOL)
	if err != nil {
		common.Errorf("ClustMgrAgent::Fail to init ClusterInfoCache : %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}
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

	c.createNotifyCh, err = mgr.StartListenIndexCreate("ClustMgrAgent")
	if err != nil {
		common.Errorf("ClustMgrAgent::NewClustMgrAgent Error In "+
			"StartListenIndexCreate %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	c.dropNotifyCh, err = mgr.StartListenIndexDelete("ClustMgrAgent")
	if err != nil {
		common.Errorf("ClustMgrAgent::NewClustMgrAgent Error In "+
			"StartListenIndexDelete %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}
	//start clustMgrAgent loop which listens to commands from its supervisor
	go c.run()

	go c.listenIndexManagerMsgs()

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

	case CBQ_CREATE_INDEX_DDL:
		c.handleCreateIndex(cmd)

	case CBQ_DROP_INDEX_DDL:
		c.handleDropIndex(cmd)

	default:
		common.Errorf("ClusterMgrAgent::handleSupvervisorCommands Unknown Message %v", cmd)
	}

}

func (c *clustMgrAgent) listenIndexManagerMsgs() {

	for {
		select {

		case data, ok := <-c.createNotifyCh:
			if ok {
				//unmarshal and send to indexer
				common.Debugf("clustMgrAgent::listenIndexManagerMsgs Notification " +
					"Received for Create Index")
				idxDefn, err := manager.UnmarshallIndexDefn(data.([]byte))
				if err == nil {

					pc := common.NewKeyPartitionContainer()

					//Add one partition for now
					addr := net.JoinHostPort("", c.config["streamMaintPort"].String())
					endpt := []common.Endpoint{common.Endpoint(addr)}

					partnDefn := common.KeyPartitionDefn{Id: common.PartitionId(1),
						Endpts: endpt}
					pc.AddPartition(common.PartitionId(1), partnDefn)

					idxInst := common.IndexInst{InstId: common.IndexInstId(idxDefn.DefnId),
						Defn:  *idxDefn,
						State: common.INDEX_STATE_INITIAL,
						Pc:    pc,
					}

					c.supvRespch <- &MsgCreateIndex{mType: CLUST_MGR_CREATE_INDEX_DDL,
						indexInst: idxInst}
				}
			} else {

				common.Errorf("clustMgrAgent::listenIndexManagerMsgs Unexpected " +
					"Close Received from Index Manager.")
				c.supvRespch <- &MsgError{
					err: Error{code: ERROR_INDEX_MANAGER_CHANNEL_CLOSE,
						severity: FATAL,
						category: CLUSTER_MGR}}
			}

		case data, ok := <-c.dropNotifyCh:
			//unmarshal and send to indexer
			if ok {
				common.Debugf("clustMgrAgent::listenIndexManagerMsgs Notification " +
					"Received for Drop Index")
				idxKey := data.(string)
				id, err := strconv.ParseUint(idxKey, 10, 64)
				if err != nil {
					idxDefn, err := c.mgr.GetIndexDefnById(common.IndexDefnId(id))
					if err == nil {
						c.supvRespch <- &MsgDropIndex{mType: CLUST_MGR_DROP_INDEX_DDL,
							indexInstId: common.IndexInstId(idxDefn.DefnId)}
					} else {
						common.Errorf("clustMgrAgent::listenIndexManagerMsgs Unable to find"+
							"Index. Key %v. Error %v", idxKey, err)
					}
				}

			} else {
				common.Errorf("clustMgrAgent::listenIndexManagerMsgs Unexpected " +
					"Close Received from Index Manager.")
				c.supvRespch <- &MsgError{
					err: Error{code: ERROR_INDEX_MANAGER_CHANNEL_CLOSE,
						severity: FATAL,
						category: CLUSTER_MGR}}
			}

		}
	}

}

func (c *clustMgrAgent) handleCreateIndex(cmd Message) {

	idxInst := cmd.(*MsgCreateIndex).GetIndexInst()

	err := c.mgr.HandleCreateIndexDDL(&idxInst.Defn)
	if err != nil {
		common.Errorf("ClustMgrAgent::handleCreateIndex Error In Create Index %v", err)
		c.supvCmdch <- &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_CREATE_FAIL,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	} else {
		c.supvCmdch <- &MsgSuccess{}
	}
}

func (c *clustMgrAgent) handleDropIndex(cmd Message) {

	idxInstId := cmd.(*MsgDropIndex).GetIndexInstId()

	err := c.mgr.HandleDeleteIndexDDL(common.IndexDefnId(idxInstId))
	if err != nil {
		common.Errorf("ClustMgrAgent::handleDropIndex Error In Drop Index %v", err)
		c.supvCmdch <- &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_DROP_FAIL,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	} else {
		c.supvCmdch <- &MsgSuccess{}
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
