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
	"github.com/couchbase/cbauth/metakv"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"strconv"
	"strings"
	"sync"
	"time"
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

type IndexerVersionToken struct {
	Version uint64
}

type IndexerStorageModeToken struct {
	NodeUUID         string
	Override         string
	LocalStorageMode string
}

type CommandListener struct {
	doCreate     bool
	doBuild      bool
	doDelete     bool
	createTokens map[string]*CreateCommandToken
	buildTokens  map[string]*BuildCommandToken
	deleteTokens map[string]*DeleteCommandToken
	mutex        sync.Mutex
	cancelCh     chan struct{}
	donech       chan bool
}

//////////////////////////////////////////////////////////////
// Create Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostCreateCommandToken(defnId c.IndexDefnId, bucketUUID string, defns map[c.IndexerId][]c.IndexDefn) error {

	commandToken := &CreateCommandToken{
		DefnId:      defnId,
		BucketUUID:  bucketUUID,
		Definitions: defns,
	}

	id := fmt.Sprintf("%v", defnId)
	return c.MetakvBigValueSet(CreateDDLCommandTokenPath+id, commandToken)
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func CreateCommandTokenExist(defnId c.IndexDefnId) (bool, error) {

	token, err := FetchCreateCommandToken(defnId)
	if err != nil {
		return false, err
	}

	return token != nil, nil
}

//
// Does token exist? Return true only if token exist and there is no error.
//
func DeleteCreateCommandToken(defnId c.IndexDefnId) error {

	id := fmt.Sprintf("%v", defnId)
	return c.MetakvBigValueDel(CreateDDLCommandTokenPath + id)
}

//
// Fetch create command token
// This function take metakv path
//
func FetchCreateCommandToken(defnId c.IndexDefnId) (*CreateCommandToken, error) {

	token := &CreateCommandToken{}

	id := fmt.Sprintf("%v", defnId)
	exists, err := c.MetakvBigValueGet(CreateDDLCommandTokenPath+id, token)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return token, nil
}

func ListCreateCommandToken() ([]string, error) {

	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	return paths, nil
}

func GetDefnIdFromCreateCommandTokenPath(path string) (c.IndexDefnId, error) {

	if len(path) <= len(CreateDDLCommandTokenPath) {
		return c.IndexDefnId(0), fmt.Errorf("Invalid path %v", path)
	}

	path = path[len(CreateDDLCommandTokenPath):]

	loc := strings.LastIndex(path, "/")
	if loc != -1 {
		path = path[:loc]
	}

	id, err := strconv.ParseUint(path, 10, 64)
	if err != nil {
		return c.IndexDefnId(0), err
	}

	return c.IndexDefnId(id), nil
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

func NewCommandListener(donech chan bool, doCreate bool, doBuild bool, doDelete bool) *CommandListener {

	return &CommandListener{
		doCreate:     doCreate,
		doBuild:      doBuild,
		doDelete:     doDelete,
		createTokens: make(map[string]*CreateCommandToken),
		buildTokens:  make(map[string]*BuildCommandToken),
		deleteTokens: make(map[string]*DeleteCommandToken),
		cancelCh:     make(chan struct{}),
		donech:       donech,
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

func (m *CommandListener) HasNewCreateTokens() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.createTokens) != 0
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
		if strings.Contains(path, DeleteDDLCommandTokenPath) {
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
		delete(m.createTokens, path)
		return
	}

	defnId, err := GetDefnIdFromCreateCommandTokenPath(path)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process create index token.  Skip %v.  Internal Error = %v.", path, err)
		return
	}

	token, err := FetchCreateCommandToken(defnId)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process create index token.  Skip %v.  Internal Error = %v.", path, err)
		return
	}

	if token != nil {
		m.AddNewCreateToken(path, token)
	}
}

func (m *CommandListener) handleNewBuildCommandToken(path string, value []byte) {

	if !m.doBuild {
		return
	}

	if value == nil {
		delete(m.buildTokens, path)
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
		delete(m.deleteTokens, path)
		return
	}

	token, err := UnmarshallDeleteCommandToken(value)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process delete index token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	m.AddNewDeleteToken(path, token)
}
