// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

//AdminManager listens to the admin port messages and relays it back to Indexer
type AdminManager interface {
}

type adminMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

}

func NewAdminManager(supvCmdch MsgChannel, supvRespch MsgChannel) (
	AdminManager, Message) {

	//Init the adminMgr struct
	a := &adminMgr{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
	}

	//start Admin Manager loop which listens to commands from its supervisor
	go a.run()

	return a, nil

}

//run starts the admin manager loop which listens to messages
//from it supervisor(indexer)
func (a *adminMgr) run() {

	//main Admin Manager loop
loop:
	for {
		select {

		case cmd, ok := <-a.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					break loop
				}
				a.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (a *adminMgr) handleSupvervisorCommands(cmd Message) {
	//TODO

}
