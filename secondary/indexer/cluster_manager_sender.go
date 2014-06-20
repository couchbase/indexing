// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

//ClustMgrSender provides the mechanism to talk to Index Coordinator
type ClustMgrSender interface {
}

type clustMgrSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

}

func NewClustMgrSender(supvCmdch MsgChannel, supvRespch MsgChannel) (
	ClustMgrSender, Message) {

	//Init the clustMgrSender struct
	c := &clustMgrSender{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
	}

	//start clustMgrSender loop which listens to commands from its supervisor
	go c.run()

	return c, nil

}

//run starts the clustmgrsender loop which listens to messages
//from it supervisor(indexer)
func (c *clustMgrSender) run() {

	//main ClustMgrSender loop
loop:
	for {
		select {

		case cmd, ok := <-c.supvCmdch:
			if ok {
				if cmd.GetMsgType() == CLUST_MGR_SENDER_SHUTDOWN {
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

func (c *clustMgrSender) handleSupvervisorCommands(cmd Message) {
	//TODO

}
