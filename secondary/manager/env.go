// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"bytes"
	json "encoding/json"
	co "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/gometa/common"
	"net"
	"os"
	"strings"
)

type env struct {
	hostUDPAddr     net.Addr
	hostTCPAddr     net.Addr
	hostRequestAddr net.Addr
	peerUDPAddr     []string
	peerTCPAddr     []string
}

type node struct {
	ElectionAddr string
	MessageAddr  string
	RequestAddr  string
}

type config struct {
	Host *node
	Peer []*node
}

func newEnv(config string) (e *env, err error) {
	e = new(env)

	if config == "" {
		err = e.initWithArgs()
		if err != nil {
			return nil, err
		}
	}

	err = e.initWithConfig(config)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *env) getHostUDPAddr() string {
	return e.hostUDPAddr.String()
}

func (e *env) getHostElectionPort() string {

	_, port, err := net.SplitHostPort(e.hostUDPAddr.String())
	if err != nil {
		port = e.hostUDPAddr.String()
	}

	return port
}


func (e *env) getHostTCPAddr() string {
	return e.hostTCPAddr.String()
}

func (e *env) getHostRequestAddr() string {
	return e.hostRequestAddr.String()
}

func (e *env) getPeerUDPAddr() []string {
	return e.peerUDPAddr
}

func (e *env) getPeerTCPAddr() []string {
	return e.peerTCPAddr
}

func (e *env) findMatchingPeerTCPAddr(updAddr string) string {
	for i := 0; i < len(e.peerUDPAddr); i++ {
		if e.peerUDPAddr[i] == updAddr {
			return e.peerTCPAddr[i]
		}
	}
	return ""
}

func (e *env) findMatchingPeerUDPAddr(tcpAddr string) string {
	for i := 0; i < len(e.peerTCPAddr); i++ {
		if e.peerTCPAddr[i] == tcpAddr {
			return e.peerUDPAddr[i]
		}
	}
	return ""
}

func (e *env) initWithConfig(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	_, err = buffer.ReadFrom(file)
	if err != nil {
		return err
	}

	var c config
	err = json.Unmarshal(buffer.Bytes(), &c)
	if err != nil {
		return err
	}

	if e.hostUDPAddr, err = resolveAddr(common.ELECTION_TRANSPORT_TYPE, c.Host.ElectionAddr); err != nil {
		return err
	}
	co.Debugf("Env.initWithConfig(): Host UDP Addr %s", e.hostUDPAddr.String())

	if e.hostTCPAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, c.Host.MessageAddr); err != nil {
		return err
	}
	co.Debugf("Env.initWithConfig(): Host TCP Addr %s", e.hostTCPAddr.String())

	if e.hostRequestAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, c.Host.RequestAddr); err != nil {
		return err
	}
	co.Debugf("Env.initWithConfig(): Host Request Addr %s", e.hostRequestAddr.String())

	e.peerUDPAddr = make([]string, 0, len(c.Peer))
	e.peerTCPAddr = make([]string, 0, len(c.Peer))

	for _, peer := range c.Peer {
		udpAddr, err := resolveAddr(common.ELECTION_TRANSPORT_TYPE, peer.ElectionAddr)
		if err != nil {
			return err
		}
		e.peerUDPAddr = append(e.peerUDPAddr, udpAddr.String())
		co.Debugf("Env.initWithConfig(): Peer UDP Addr %s", udpAddr.String())

		tcpAddr, err := resolveAddr(common.MESSAGE_TRANSPORT_TYPE, peer.MessageAddr)
		if err != nil {
			return err
		}
		e.peerTCPAddr = append(e.peerTCPAddr, tcpAddr.String())
		co.Debugf("Env.initWithConfig(): Peer TCP Addr %s", tcpAddr.String())
	}

	return nil
}

func (e *env) initWithArgs() error {
	if len(os.Args) < 3 {
		return NewError(ERROR_ARGUMENTS, NORMAL, GENERIC, nil, 
					"Missing command line argument")
	}

	err := e.resolveHostAddr()
	if err != nil {
		return err
	}

	if len(os.Args) >= 6 {
		e.resolvePeerAddr()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *env) resolveHostAddr() (err error) {

	e.hostUDPAddr, err = resolveAddr(common.ELECTION_TRANSPORT_TYPE, os.Args[1])
	if err != nil {
		return err
	}
	co.Debugf("Env.resoleHostAddr(): Host UDP Addr %s", e.hostUDPAddr.String())

	e.hostTCPAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, os.Args[2])
	if err != nil {
		return err
	}
	co.Debugf("Env.resolveHostAddr(): Host TCP Addr %s", e.hostTCPAddr.String())

	e.hostRequestAddr, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, os.Args[3])
	if err != nil {
		return err
	}
	co.Debugf("Env.resolveHostAddr(): Host Request Addr %s", e.hostRequestAddr.String())

	return nil
}

func (e *env) resolvePeerAddr() error {
	args := os.Args[4:]
	e.peerUDPAddr = make([]string, 0, len(args))
	e.peerTCPAddr = make([]string, 0, len(args))

	for i := 0; i < len(args); {
		peer, err := resolveAddr(common.ELECTION_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err
		}
		e.peerUDPAddr = append(e.peerUDPAddr, peer.String())
		i++
		co.Debugf("Env.resolvePeerAddr(): Peer UDP Addr %s", peer.String())

		peer, err = resolveAddr(common.MESSAGE_TRANSPORT_TYPE, args[i])
		if err != nil {
			return err
		}
		e.peerTCPAddr = append(e.peerTCPAddr, peer.String())
		i++
		co.Debugf("Env.resolvePeerAddr(): Peer TCP Addr %s", peer.String())
	}

	return nil
}

func resolveAddr(network string, addr string) (addrObj net.Addr, err error) {

	if strings.Contains(network, common.MESSAGE_TRANSPORT_TYPE) {
		addrObj, err = net.ResolveTCPAddr(network, addr)
	} else {
		addrObj, err = net.ResolveUDPAddr(network, addr)
	}

	if err != nil {
		return nil, err
	}

	return addrObj, nil
}
