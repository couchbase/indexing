// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth/metakv"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////////////////////////////////
// Const
////////////////////////////////////////////////////////////////////////

const DDLMetakvDir = c.IndexingMetaDir + "ddl/"
const InfoMetakvDir = c.IndexingMetaDir + "info/"
const CommandMetakvDir = DDLMetakvDir + "commandToken/"

const CreateDDLCommandTokenTag = "create/"
const CreateDDLCommandTokenPath = CommandMetakvDir + CreateDDLCommandTokenTag

const DeleteDDLCommandTokenTag = "delete/"
const DeleteDDLCommandTokenPath = CommandMetakvDir + DeleteDDLCommandTokenTag

const BuildDDLCommandTokenTag = "build/"
const BuildDDLCommandTokenPath = CommandMetakvDir + BuildDDLCommandTokenTag

const DropInstanceDDLCommandTokenTag = "dropInstance/"
const DropInstanceDDLCommandTokenPath = CommandMetakvDir + DropInstanceDDLCommandTokenTag

const IndexerVersionTokenTag = "versionToken"
const IndexerVersionTokenPath = InfoMetakvDir + IndexerVersionTokenTag

const IndexerStorageModeTokenTag = "storageModeToken/"
const IndexerStorageModeTokenPath = InfoMetakvDir + IndexerStorageModeTokenTag

//////////////////////////////////////////////////////////////
// Concrete Type
//
// These are immutable tokens that are pushed to metakv
// during DDL.  Since these tokens are immutable, we do
// not have to worry about indexers seeing different token
// values.  An indexer either does not see the value, or
// seeing the same value as other indexers.
//
//////////////////////////////////////////////////////////////

type CreateCommandTokenList struct {
	Tokens []CreateCommandToken
}

type CreateCommandToken struct {
	DefnId      c.IndexDefnId
	BucketUUID  string
	Definitions map[c.IndexerId][]c.IndexDefn
	RequestId   uint64
}

type DeleteCommandTokenList struct {
	Tokens []DeleteCommandToken
}

type DeleteCommandToken struct {
	Name   string
	Bucket string
	DefnId c.IndexDefnId
}

type BuildCommandToken struct {
	Name   string
	Bucket string
	DefnId c.IndexDefnId
}

type DropInstanceCommandTokenList struct {
	Tokens []DropInstanceCommandToken
}

type DropInstanceCommandToken struct {
	DefnId    c.IndexDefnId
	InstId    c.IndexInstId
	ReplicaId int
	Defn      c.IndexDefn
}

type IndexerVersionToken struct {
	Version uint64
}

type IndexerStorageModeToken struct {
	NodeUUID         string
	Override         string
	LocalStorageMode string
}

type CommandListener struct {
	doCreate       bool
	hasNewCreate   bool
	doBuild        bool
	doDelete       bool
	doDropInst     bool
	createTokens   map[string]*CreateCommandToken
	buildTokens    map[string]*BuildCommandToken
	deleteTokens   map[string]*DeleteCommandToken
	dropInstTokens map[string]*DropInstanceCommandToken
	mutex          sync.Mutex
	cancelCh       chan struct{}
	donech         chan bool
}

//////////////////////////////////////////////////////////////
// Create Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostCreateCommandToken(defnId c.IndexDefnId, bucketUUID string, requestId uint64, defns map[c.IndexerId][]c.IndexDefn) error {

	commandToken := &CreateCommandToken{
		DefnId:      defnId,
		BucketUUID:  bucketUUID,
		Definitions: defns,
		RequestId:   requestId,
	}

	var id string
	if requestId == 0 {
		id = fmt.Sprintf("%v", defnId)
	} else {
		id = fmt.Sprintf("%v-%v", defnId, requestId)
	}
	return c.MetakvBigValueSet(CreateDDLCommandTokenPath+id, commandToken)
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func CreateCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	id := fmt.Sprintf("%v", defnId)
	commandToken := &CreateCommandToken{}
	return c.MetakvBigValueGet(CreateDDLCommandTokenPath+id, commandToken)
}

//
// Delete create command token
//
func DeleteCreateCommandToken(defnId c.IndexDefnId, requestId uint64) error {

	var id string
	if requestId == 0 {
		id = fmt.Sprintf("%v", defnId)
	} else {
		id = fmt.Sprintf("%v-%v", defnId, requestId)
	}
	return c.MetakvBigValueDel(CreateDDLCommandTokenPath + id)
}

//
// Delete all create command token
//
func DeleteAllCreateCommandToken(defnId c.IndexDefnId) error {

	tokens, err := ListAndFetchCreateCommandToken(defnId)
	if err != nil {
		return err
	}

	for _, token := range tokens {
		DeleteCreateCommandToken(token.DefnId, token.RequestId)
	}

	return nil
}

//
// Fetch create command token
// This function take metakv path
//
func FetchCreateCommandToken(defnId c.IndexDefnId, requestId uint64) (*CreateCommandToken, error) {

	token := &CreateCommandToken{}

	var id string
	if requestId == 0 {
		id = fmt.Sprintf("%v", defnId)
	} else {
		id = fmt.Sprintf("%v-%v", defnId, requestId)
	}
	exists, err := c.MetakvBigValueGet(CreateDDLCommandTokenPath+id, token)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return token, nil
}

// FetchIndexDefnToCreateCommandTokensMap will get a map of all Index Definition & CreateCommand
// tokens present in metakv
func FetchIndexDefnToCreateCommandTokensMap() (map[c.IndexDefnId][]*CreateCommandToken, error) {

	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	var result map[c.IndexDefnId][]*CreateCommandToken
	if len(paths) > 0 {
		result = make(map[c.IndexDefnId][]*CreateCommandToken)
		for _, path := range paths {
			defnID, requestID, err := GetDefnIdFromCreateCommandTokenPath(path)
			if err != nil {
				logging.Errorf("FetchIndexDefnToCreateCommandTokenMap: Failed to process create index token %v.  Internal Error = %v.", path, err)
				return nil, err
			}

			token, err := FetchCreateCommandToken(defnID, requestID)
			if err != nil {
				logging.Errorf("FetchIndexDefnToCreateCommandTokenMap: Failed to process create index token %v.  Internal Error = %v.", path, err)
				return nil, err
			}

			if token != nil {
				result[defnID] = append(result[defnID], token)
			}
		}
	}

	return result, nil
}

func ListCreateCommandToken() ([]string, error) {

	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	return paths, nil
}

func ListAndFetchCreateCommandToken(defnId c.IndexDefnId) ([]*CreateCommandToken, error) {

	id := fmt.Sprintf("%v", defnId)
	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath + id)
	if err != nil {
		return nil, err
	}

	var result []*CreateCommandToken
	if len(paths) > 0 {

		result = make([]*CreateCommandToken, 0, len(paths))
		for _, path := range paths {
			token := &CreateCommandToken{}
			exists, err := c.MetakvBigValueGet(path, token)
			if err != nil {
				logging.Errorf("ListAndFetchCreateCommandToken: path %v err %v", path, err)
				return nil, err
			}

			if exists {
				result = append(result, token)
			}
		}
	}

	return result, nil
}

func GetDefnIdFromCreateCommandTokenPath(path string) (c.IndexDefnId, uint64, error) {

	if len(path) <= len(CreateDDLCommandTokenPath) {
		return c.IndexDefnId(0), 0, fmt.Errorf("Invalid path %v", path)
	}

	path = path[len(CreateDDLCommandTokenPath):]

	// Get DefnId
	var defnId c.IndexDefnId
	if len(path) != 0 {
		var defnIdStr string

		if loc := strings.Index(path, "-"); loc != -1 {
			defnIdStr = path[:loc]

			if loc != len(path)-1 {
				path = path[loc+1:]
			} else {
				path = ""
			}
		} else {
			if loc := strings.Index(path, "/"); loc != -1 {
				path = path[:loc]
			}

			defnIdStr = path
			path = ""
		}

		temp, err := strconv.ParseUint(defnIdStr, 10, 64)
		if err != nil {
			return c.IndexDefnId(0), 0, err
		}
		defnId = c.IndexDefnId(temp)
	}

	// Get requestId
	var requestId uint64
	if len(path) != 0 {

		if loc := strings.Index(path, "/"); loc != -1 {
			path = path[:loc]
		}

		temp, err := strconv.ParseUint(path, 10, 64)
		if err != nil {
			return c.IndexDefnId(0), 0, err
		}
		requestId = uint64(temp)
	}

	return c.IndexDefnId(defnId), requestId, nil
}

//
// Unmarshall
//
func UnmarshallCreateCommandTokenList(data []byte) (*CreateCommandTokenList, error) {

	r := new(CreateCommandTokenList)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallCreateCommandTokenList(r *CreateCommandTokenList) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Delete Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostDeleteCommandToken(defnId c.IndexDefnId) error {

	commandToken := &DeleteCommandToken{
		DefnId: defnId,
	}

	id := fmt.Sprintf("%v", defnId)
	if err := c.MetakvSet(DeleteDDLCommandTokenPath+id, commandToken); err != nil {
		return errors.New(fmt.Sprintf("Fail to delete index.  Internal Error = %v", err))
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func DeleteCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	commandToken := &DeleteCommandToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvGet(DeleteDDLCommandTokenPath+id, commandToken)
}

//
// ListDeleteCommandToken returns all delete tokens for this indexer host.
// Result is nil if no tokens.
//
func ListDeleteCommandToken() ([]*DeleteCommandToken, error) {
	entries, err := c.MetakvList(DeleteDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	var result []*DeleteCommandToken
	if len(entries) != 0 {
		result = make([]*DeleteCommandToken, 0, len(entries))
		for _, entry := range entries {
			token := new(DeleteCommandToken)
			err = json.Unmarshal(entry.Value, token)
			if err != nil {
				return nil, err
			}
			result = append(result, token)
		}
	}

	return result, nil
}

//
// Unmarshall
//
func UnmarshallDeleteCommandToken(data []byte) (*DeleteCommandToken, error) {

	r := new(DeleteCommandToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallDeleteCommandToken(r *DeleteCommandToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func UnmarshallDeleteCommandTokenList(data []byte) (*DeleteCommandTokenList, error) {

	r := new(DeleteCommandTokenList)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallDeleteCommandTokenList(r *DeleteCommandTokenList) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

/////////////////////////////////////////////////////////////
// Build Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostBuildCommandToken(defnId c.IndexDefnId) error {

	commandToken := &BuildCommandToken{
		DefnId: defnId,
	}

	id := fmt.Sprintf("%v", defnId)
	if err := c.MetakvSet(BuildDDLCommandTokenPath+id, commandToken); err != nil {
		return errors.New(fmt.Sprintf("Fail to buildindex.  Internal Error = %v", err))
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func BuildCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	commandToken := &BuildCommandToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvGet(BuildDDLCommandTokenPath+id, commandToken)
}

//
// Unmarshall
//
func UnmarshallBuildCommandToken(data []byte) (*BuildCommandToken, error) {

	r := new(BuildCommandToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

//
// Marshall
//
func MarshallBuildCommandToken(r *BuildCommandToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Drop Instance Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostDropInstanceCommandToken(defnId c.IndexDefnId, instId c.IndexInstId, replicaId int, defn c.IndexDefn) error {

	commandToken := &DropInstanceCommandToken{
		DefnId:    defnId,
		InstId:    instId,
		ReplicaId: replicaId,
		Defn:      defn,
	}

	id := fmt.Sprintf("%v/%v", defnId, instId)
	if err := c.MetakvBigValueSet(DropInstanceDDLCommandTokenPath+id, commandToken); err != nil {
		return errors.New(fmt.Sprintf("Fail to drop index instance.  Internal Error = %v", err))
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func DropInstanceCommandTokenExist(defnId c.IndexDefnId, instId c.IndexInstId) (bool, error) {

	commandToken := &DropInstanceCommandToken{}
	id := fmt.Sprintf("%v/%v", defnId, instId)
	return c.MetakvBigValueGet(DropInstanceDDLCommandTokenPath+id, commandToken)
}

//
// Return the list of drop instance command token for a given index
//
func ListAndFetchDropInstanceCommandToken(defnId c.IndexDefnId) ([]*DropInstanceCommandToken, error) {

	id := fmt.Sprintf("%v/", defnId)
	paths, err := c.MetakvBigValueList(DropInstanceDDLCommandTokenPath + id)
	if err != nil {
		return nil, err
	}

	var result []*DropInstanceCommandToken
	if len(paths) > 0 {
		result = make([]*DropInstanceCommandToken, 0, len(paths))
		for _, path := range paths {
			token := &DropInstanceCommandToken{}
			exists, err := c.MetakvBigValueGet(path, token)
			if err != nil {
				logging.Errorf("ListDropInstanceCommandToken: path %v err %v", path, err)
				return nil, err
			}

			if exists {
				result = append(result, token)
			}
		}
	}

	return result, nil
}

func ListAndFetchAllDropInstanceCommandToken() ([]*DropInstanceCommandToken, error) {

	paths, err := c.MetakvBigValueList(DropInstanceDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	var result []*DropInstanceCommandToken
	if len(paths) > 0 {
		result = make([]*DropInstanceCommandToken, 0, len(paths))
		for _, path := range paths {
			token := &DropInstanceCommandToken{}
			exists, err := c.MetakvBigValueGet(path, token)
			if err != nil {
				logging.Errorf("ListAllDropInstanceCommandToken: path %v err %v", path, err)
				return nil, err
			}

			if exists {
				result = append(result, token)
			}
		}
	}

	return result, nil
}

// FetchIndexDefnToDropInstanceCommandTokenMap will get a map of all Index Definition & CreateCommand
// tokens present in metakv
func FetchIndexDefnToDropInstanceCommandTokenMap() (map[c.IndexDefnId][]*DropInstanceCommandToken, error) {
	paths, err := c.MetakvBigValueList(DropInstanceDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	var result map[c.IndexDefnId][]*DropInstanceCommandToken
	if len(paths) > 0 {
		result = make(map[c.IndexDefnId][]*DropInstanceCommandToken)
		for _, path := range paths {
			token := &DropInstanceCommandToken{}
			exists, err := c.MetakvBigValueGet(path, token)
			if err != nil {
				logging.Errorf("FetchIndexDefnToDropInstanceCommandTokenMap: path %v err %v", path, err)
				return nil, err
			}

			if exists {
				result[token.DefnId] = append(result[token.DefnId], token)
			}
		}
	}

	return result, nil
}

//
// Unmarshall
//
func UnmarshallDropInstanceCommandToken(data []byte) (*DropInstanceCommandToken, error) {

	r := new(DropInstanceCommandToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallDropInstanceCommandToken(r *DropInstanceCommandToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func UnmarshallDropInstanceCommandTokenList(data []byte) (*DropInstanceCommandTokenList, error) {

	r := new(DropInstanceCommandTokenList)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallDropInstanceCommandTokenList(r *DropInstanceCommandTokenList) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Version Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for indexer version
//
func PostIndexerVersionToken(version uint64) error {

	token := &IndexerVersionToken{
		Version: version,
	}

	if err := c.MetakvSet(IndexerVersionTokenPath, token); err != nil {
		logging.Errorf("Fail to post indexer version to metakv.  Internal Error = %v", err)
		return err
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerVersionToken() (uint64, error) {

	token := &IndexerVersionToken{}
	found, err := c.MetakvGet(IndexerVersionTokenPath, token)
	if err != nil {
		logging.Errorf("Fail to get indexer version from metakv.  Internal Error = %v", err)
		return 0, err
	}

	if !found {
		return 0, nil
	}

	return token.Version, nil
}

//
// Unmarshall
//
func UnmarshallIndexerVersionToken(data []byte) (*IndexerVersionToken, error) {

	r := new(IndexerVersionToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallIndexerVersionToken(r *IndexerVersionToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// Storage Mode Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for indexer storage mode
//
func PostIndexerStorageModeOverride(nodeUUID string, override string) error {

	if len(nodeUUID) == 0 {
		return errors.New("NodeUUId is not specified. Fail to set storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	if token == nil {
		token = &IndexerStorageModeToken{
			NodeUUID: nodeUUID,
		}
	}
	token.Override = override

	if err := c.MetakvSet(IndexerStorageModeTokenPath+nodeUUID, token); err != nil {
		logging.Errorf("Fail to post indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	return nil
}

//
// Generate a token to metakv for indexer storage mode
//
func PostIndexerLocalStorageMode(nodeUUID string, storageMode c.StorageMode) error {

	if len(nodeUUID) == 0 {
		return errors.New("NodeUUId is not specified. Fail to set local storage mode in metakv.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	if token == nil {
		token = &IndexerStorageModeToken{
			NodeUUID: nodeUUID,
		}
	}

	token.LocalStorageMode = string(c.StorageModeToIndexType(storageMode))

	if err := c.MetakvSet(IndexerStorageModeTokenPath+nodeUUID, token); err != nil {
		logging.Errorf("Fail to post indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return err
	}

	return nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerStorageModeToken(nodeUUID string) (*IndexerStorageModeToken, error) {

	if len(nodeUUID) == 0 {
		return nil, errors.New("NodeUUId is not specified. Fail to get storage mode token.")
	}

	token := &IndexerStorageModeToken{}
	found, err := c.MetakvGet(IndexerStorageModeTokenPath+nodeUUID, token)
	if err != nil {
		logging.Errorf("Fail to get indexer storage token from metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return nil, err
	}

	if !found {
		return nil, nil
	}

	return token, nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerStorageModeOverride(nodeUUID string) (string, error) {

	if len(nodeUUID) == 0 {
		return "", errors.New("NodeUUId is not specified. Fail to get storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return "", err
	}

	if token != nil {
		return token.Override, nil
	}

	return "", nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func GetIndexerLocalStorageMode(nodeUUID string) (c.StorageMode, error) {

	if len(nodeUUID) == 0 {
		return c.NOT_SET, errors.New("NodeUUId is not specified. Fail to get storage mode override.")
	}

	token, err := GetIndexerStorageModeToken(nodeUUID)
	if err != nil {
		logging.Errorf("Fail to read indexer storage mode to metakv for node %v.  Internal Error = %v", nodeUUID, err)
		return c.NOT_SET, err
	}

	if token != nil {
		return c.IndexTypeToStorageMode(c.IndexType(token.LocalStorageMode)), nil
	}

	return c.NOT_SET, nil
}

//
//
// Unmarshall
//
func UnmarshallIndexerStorageModeToken(data []byte) (*IndexerStorageModeToken, error) {

	r := new(IndexerStorageModeToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func MarshallIndexerStorageModeToken(r *IndexerStorageModeToken) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

//////////////////////////////////////////////////////////////
// CommandListener
//////////////////////////////////////////////////////////////

func NewCommandListener(donech chan bool, doCreate bool, doBuild bool, doDelete bool, doDropInst bool) *CommandListener {

	return &CommandListener{
		doCreate:       doCreate,
		doBuild:        doBuild,
		doDelete:       doDelete,
		doDropInst:     doDropInst,
		createTokens:   make(map[string]*CreateCommandToken),
		buildTokens:    make(map[string]*BuildCommandToken),
		deleteTokens:   make(map[string]*DeleteCommandToken),
		dropInstTokens: make(map[string]*DropInstanceCommandToken),
		cancelCh:       make(chan struct{}),
		donech:         donech,
	}
}

func (m *CommandListener) Close() {
	close(m.cancelCh)
}

func (m *CommandListener) GetNewCreateTokens() map[string]*CreateCommandToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.createTokens) != 0 {
		result := m.createTokens
		m.createTokens = make(map[string]*CreateCommandToken)
		return result
	}

	return nil
}

func (m *CommandListener) HasCreateTokens() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.createTokens) != 0
}

func (m *CommandListener) HasNewCreateTokens() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	result := m.hasNewCreate
	m.hasNewCreate = false
	return result
}

func (m *CommandListener) AddNewCreateToken(path string, token *CreateCommandToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.createTokens[path] = token
}

func (m *CommandListener) GetNewDeleteTokens() map[string]*DeleteCommandToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.deleteTokens) != 0 {
		result := m.deleteTokens
		m.deleteTokens = make(map[string]*DeleteCommandToken)
		return result
	}

	return nil
}

func (m *CommandListener) AddNewDeleteToken(path string, token *DeleteCommandToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.deleteTokens[path] = token
}

func (m *CommandListener) GetNewDropInstanceTokens() map[string]*DropInstanceCommandToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.dropInstTokens) != 0 {
		result := m.dropInstTokens
		m.dropInstTokens = make(map[string]*DropInstanceCommandToken)
		return result
	}

	return nil
}

func (m *CommandListener) AddNewDropInstanceToken(path string, token *DropInstanceCommandToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.dropInstTokens[path] = token
}

func (m *CommandListener) GetNewBuildTokens() map[string]*BuildCommandToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.buildTokens) != 0 {
		result := m.buildTokens
		m.buildTokens = make(map[string]*BuildCommandToken)
		return result
	}

	return nil
}

func (m *CommandListener) AddNewBuildToken(path string, token *BuildCommandToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.buildTokens[path] = token
}

func (m *CommandListener) ListenTokens() {

	metaKVCallback := func(path string, value []byte, rev interface{}) error {
		if strings.Contains(path, DropInstanceDDLCommandTokenPath) {
			m.handleNewDropInstanceCommandToken(path, value)

		} else if strings.Contains(path, DeleteDDLCommandTokenPath) {
			m.handleNewDeleteCommandToken(path, value)

		} else if strings.Contains(path, BuildDDLCommandTokenPath) {
			m.handleNewBuildCommandToken(path, value)

		} else if strings.Contains(path, CreateDDLCommandTokenPath) {
			m.handleNewCreateCommandToken(path, value)
		}

		return nil
	}

	go func() {
		defer close(m.donech)

		fn := func(r int, err error) error {
			if r > 0 {
				logging.Errorf("CommandListener: metakv notifier failed (%v)..Restarting %v", err, r)
			}
			err = metakv.RunObserveChildren(CommandMetakvDir, metaKVCallback, m.cancelCh)
			return err
		}

		rh := c.NewRetryHelper(200, time.Second, 2, fn)
		err := rh.Run()
		if err != nil {
			logging.Errorf("CommandListener: metakv notifier failed even after max retries.")
		}
	}()
}

func (m *CommandListener) handleNewCreateCommandToken(path string, value []byte) {

	if !m.doCreate {
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			delete(m.createTokens, path)
		}()

		return
	}

	defnId, requestId, err := GetDefnIdFromCreateCommandTokenPath(path)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process create index token.  Skip %v.  Internal Error = %v.", path, err)
		return
	}

	token, err := FetchCreateCommandToken(defnId, requestId)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process create index token.  Skip %v.  Internal Error = %v.", path, err)
		return
	}

	if token != nil {
		m.hasNewCreate = true
		m.AddNewCreateToken(path, token)
	}
}

func (m *CommandListener) handleNewBuildCommandToken(path string, value []byte) {

	if !m.doBuild {
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			delete(m.buildTokens, path)
		}()

		return
	}

	token, err := UnmarshallBuildCommandToken(value)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process build index token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	m.AddNewBuildToken(path, token)
}

func (m *CommandListener) handleNewDeleteCommandToken(path string, value []byte) {

	if !m.doDelete {
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			delete(m.deleteTokens, path)
		}()

		return
	}

	token, err := UnmarshallDeleteCommandToken(value)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process delete index token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	m.AddNewDeleteToken(path, token)
}

func (m *CommandListener) handleNewDropInstanceCommandToken(path string, value []byte) {

	if !m.doDropInst {
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			delete(m.dropInstTokens, path)
		}()

		return
	}

	token, err := UnmarshallDropInstanceCommandToken(value)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process drop index instance token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	m.AddNewDropInstanceToken(path, token)
}
