// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
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

const ScheduleCreateTokenTag = "schedule/"
const ScheduleCreateTokenPath = CommandMetakvDir + ScheduleCreateTokenTag

const StopScheduleCreateTokenTag = "stopSchedule/"
const StopScheduleCreateTokenPath = CommandMetakvDir + StopScheduleCreateTokenTag

const PlasmaInMemoryCompressionTokenTag = "PlasmaInMemoryCompression"
const PlasmaInMemoryCompressionFeaturePath = c.IndexingSettingsFeaturesMetaPath + PlasmaInMemoryCompressionTokenTag

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
	DefnId       c.IndexDefnId
	BucketUUID   string
	ScopeId      string
	CollectionId string
	Definitions  map[c.IndexerId][]c.IndexDefn
	RequestId    uint64
}

type DeleteCommandTokenList struct {
	Tokens []DeleteCommandToken
}

type DeleteCommandToken struct {
	Name     string
	Bucket   string
	DefnId   c.IndexDefnId
	Internal bool
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

func (tok *DropInstanceCommandToken) String() string {
	return fmt.Sprintf("DefnID:%v InstID:%v Replica:%v", tok.DefnId, tok.InstId, tok.ReplicaId)
}

type IndexerVersionToken struct {
	Version uint64
}

type IndexerStorageModeToken struct {
	NodeUUID         string
	Override         string
	LocalStorageMode string
}

// TODO: Check if we can directly set UUIDs in the Defn itself.
type ScheduleCreateToken struct {
	Definition   c.IndexDefn
	Plan         map[string]interface{}
	BucketUUID   string
	ScopeId      string
	CollectionId string
	IndexerId    c.IndexerId
	Ctime        int64
}

type ScheduleCreateTokenList struct {
	Tokens []ScheduleCreateToken
}

type StopScheduleCreateToken struct {
	Ctime  int64
	Reason string
	DefnId c.IndexDefnId
}

type StopScheduleCreateTokenList struct {
	Tokens []StopScheduleCreateToken
}

// TokenPathList holds a slice of string token keys as stored
// in metakv. It is used for JSON un/marshalling.
type TokenPathList struct {
	Paths []string
}

type CommandListener struct {
	doCreate        bool
	hasNewCreate    bool
	doBuild         bool
	doDelete        bool
	doDropInst      bool
	doSchedule      bool
	doStopSched     bool
	createTokens    map[string]*CreateCommandToken
	buildTokens     map[string]*BuildCommandToken
	deleteTokens    map[string]*DeleteCommandToken
	dropInstTokens  map[string]*DropInstanceCommandToken
	scheduleTokens  map[string]*ScheduleCreateToken
	stopSchedTokens map[string]*StopScheduleCreateToken
	schedTokensDel  map[string]bool
	mutex           sync.Mutex
	cancelCh        chan struct{}
	donech          chan bool
}

type PlasmaInMemoryCompresisonToken struct {
	enabled bool
}

//////////////////////////////////////////////////////////////
// Create Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostCreateCommandToken(defnId c.IndexDefnId, bucketUUID, scopeId, collectionId string,
	requestId uint64, defns map[c.IndexerId][]c.IndexDefn) error {

	commandToken := &CreateCommandToken{
		DefnId:       defnId,
		BucketUUID:   bucketUUID,
		ScopeId:      scopeId,
		CollectionId: collectionId,
		Definitions:  defns,
		RequestId:    requestId,
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

	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	var result []*CreateCommandToken
	if len(paths) > 0 {
		result = make([]*CreateCommandToken, 0, len(paths))
		for _, path := range paths {
			defnId2, _, err := GetDefnIdFromCreateCommandTokenPath(path)
			if err != nil {
				logging.Errorf("ListAndFetchCreateCommandToken: Error parsing definationid and requestid for path %v, err %v", path, err)
				return nil, err
			}

			if defnId2 != defnId {
				continue
			}

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

func ListAndFetchAllCreateCommandTokens() (result []*CreateCommandToken, err error) {

	paths, err := c.MetakvBigValueList(CreateDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	for _, path := range paths {

		token := &CreateCommandToken{}
		exists, err := c.MetakvBigValueGet(path, token)

		if err != nil {
			logging.Errorf("ListAndFetchAllCreateCommandTokens: path %v err %v", path, err)
			return nil, err
		}

		if exists {
			result = append(result, token)
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
func PostDeleteCommandToken(defnId c.IndexDefnId, internal bool, bucketName string) error {

	commandToken := &DeleteCommandToken{
		DefnId:   defnId,
		Internal: internal,
		Bucket:   bucketName,
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

// Returns true, it Delete command token is found within configured time.
// Returns fails if Delete command token does not exist (or) it takes
// more than the configured time to read metaKV
func CheckDeleteCommandTokenWithTimeout(defnId common.IndexDefnId, durationInMilli int) bool {
	// Make a non-blocking channel so that the go-routine can exit incase
	// it takes more than configured time to read from metaKV
	respCh := make(chan bool, 1)
	if durationInMilli <= 0 { // Disable the check for "0" value
		return false
	}

	go func() {
		// This is a best effort call. Hence, do not process the error
		exists, _ := DeleteCommandTokenExist(defnId)
		respCh <- exists
	}()

	select {
	case val := <-respCh:
		return val
	case <-time.After(time.Duration(durationInMilli) * time.Millisecond):
		logging.Warnf("CheckDeleteCommandTokenWithTimeout Returning as more than %v milliseconds " +
			"have elapsed while checking delete command token")
		return false
	}
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
// ListDeleteCommandTokenPaths returns the metakv paths (keys)
// of all delete tokens for this indexer host. It does not
// bother unmarhsalling the values as caller doesn't need them.
//
func ListDeleteCommandTokenPaths() ([]string, error) {
	entries, err := c.MetakvList(DeleteDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.Path)
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

// UnmarshallTokenPathList unmarshalls a generic list of token paths.
func UnmarshallTokenPathList(data []byte) (*TokenPathList, error) {

	r := new(TokenPathList)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

// MarshallTokenPathList marshalls a generic list of token paths.
func MarshallTokenPathList(r *TokenPathList) ([]byte, error) {

	buf, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// FetchIndexDefnToDeleteCommandTokensMap retrieves all delete
// tokens from metakv into a map with key token.DefnId.
func FetchIndexDefnToDeleteCommandTokensMap() (map[c.IndexDefnId]*DeleteCommandToken, error) {
	entries, err := c.MetakvList(DeleteDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	// Do not return a nil result, even if there aren't any tokens.
	result := make(map[c.IndexDefnId]*DeleteCommandToken, len(entries))
	for _, entry := range entries {
		token := new(DeleteCommandToken)
		err = json.Unmarshal(entry.Value, token)
		if err != nil {
			return nil, err
		}
		result[token.DefnId] = token
	}
	return result, nil
}

/////////////////////////////////////////////////////////////
// Build Token Management
//////////////////////////////////////////////////////////////

//
// Generate a token to metakv for recovery purpose
//
func PostBuildCommandToken(defnId c.IndexDefnId, bucketName string) error {

	commandToken := &BuildCommandToken{
		DefnId: defnId,
		Bucket: bucketName,
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

// FetchIndexDefnToBuildCommandTokensMap retrieves all build
// tokens from metakv into a map with key token.DefnId.
func FetchIndexDefnToBuildCommandTokensMap() (map[c.IndexDefnId]*BuildCommandToken, error) {
	entries, err := c.MetakvList(BuildDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	// Do not return a nil result, even if there aren't any tokens.
	result := make(map[c.IndexDefnId]*BuildCommandToken, len(entries))
	for _, entry := range entries {
		token := new(BuildCommandToken)
		err = json.Unmarshal(entry.Value, token)
		if err != nil {
			return nil, err
		}
		result[token.DefnId] = token
	}

	return result, nil
}

func ListBuildCommandTokens() (result []*BuildCommandToken, err error) {
	entries, err := c.MetakvList(BuildDDLCommandTokenPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		token := &BuildCommandToken{}
		if err = json.Unmarshal(entry.Value, token); err != nil {
			return nil, err
		}
		result = append(result, token)
	}

	return result, nil
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

// ListAndFetchAllDropInstanceCommandToken returns all drop instance
// tokens for this indexer host.
func ListAndFetchAllDropInstanceCommandToken(retries int) ([]*DropInstanceCommandToken, error) {

	paths, err := ListDropInstanceCommandTokenPaths()
	if err != nil {
		return nil, err
	}

	var result []*DropInstanceCommandToken
	if len(paths) > 0 {
		result = make([]*DropInstanceCommandToken, 0, len(paths))
		for _, path := range paths {
			token := &DropInstanceCommandToken{}
			var exists bool

			fn := func(retryAttempt int, lastErr error) error {
				var err error

				exists, err = c.MetakvBigValueGet(path, token)
				if err != nil {
					logging.Errorf("ListAllDropInstanceCommandToken: path %v err %v", path, err)
					return err
				}

				return err
			}

			rh := c.NewRetryHelper(retries, time.Second, 1, fn)
			err = rh.Run()

			if err != nil {
				return nil, err
			}

			if exists {
				result = append(result, token)
			}
		}
	}

	return result, nil
}

// ListDropInstanceCommandTokenPaths returns the metakv paths (keys)
// of all drop instance tokens for this indexer host.
func ListDropInstanceCommandTokenPaths() ([]string, error) {
	return c.MetakvBigValueList(DropInstanceDDLCommandTokenPath)
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
				logging.Errorf("ListAllDropInstanceCommandToken: path %v err %v", path, err)
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

func GetDropInstanceTokenFromPath(path string) (*DropInstanceCommandToken, error) {
	//
	// The input path is DropInstanceDDLCommandTokenPath/DefnId/InstId/Sub-partId
	//

	comps := strings.Split(path, "/")
	if len(comps) != 8 {
		return nil, fmt.Errorf("Malformed input path to GetDropInstanceTokenFromPath %v", path)
	}

	vpath := strings.Join(comps[:len(comps)-1], "/")

	token := &DropInstanceCommandToken{}

	exists, err := c.MetakvBigValueGet(vpath, token)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return token, nil
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

//////////////////////////////////////////////////////////////////////////////
// ScheduleCreateToken
//
// When two or more index creations cannot happen synchronously, index creation
// can be scheduled in the background. Each of the tokens represent one index
// creation request.
//////////////////////////////////////////////////////////////////////////////

func PostScheduleCreateToken(idxDefn c.IndexDefn, plan map[string]interface{},
	bucketUUID, scopeId, collectionId string, indexerId c.IndexerId, ctime int64) error {

	// TODO: Do we need bucket UUID? DDL service manager does overwrite bucket UUID in idxDefn
	// if bucket UUID is changed.

	token := &ScheduleCreateToken{
		Definition:   idxDefn,
		Plan:         plan,
		BucketUUID:   bucketUUID,
		ScopeId:      scopeId,
		CollectionId: collectionId,
		IndexerId:    indexerId,
		Ctime:        ctime,
	}

	path := fmt.Sprintf("%v", idxDefn.DefnId)

	err := c.MetakvBigValueSet(ScheduleCreateTokenPath+path, token)
	if err != nil {
		return err
	}

	// At this point, the instances and partitions are not initialised.
	// Add only one event for this index with a valid index DefnId, but
	// with InstId, replicaId and partnId as ZERO.
	event := systemevent.NewDDLSystemEvent("IndexScheduledForCreation", idxDefn.DefnId,
		c.IndexInstId(0), uint64(0), uint64(0), c.IndexInstId(0), string(indexerId), "")

	systemevent.InfoEvent("Indexer", systemevent.EVENTID_INDEX_SCHED_CREATE, event)

	return nil
}

func DeleteScheduleCreateToken(defnId c.IndexDefnId) error {
	path := fmt.Sprintf("%v", defnId)
	return c.MetakvBigValueDel(ScheduleCreateTokenPath + path)
}

func ListAllScheduleCreateTokens() ([]*ScheduleCreateToken, error) {

	// TODO: Check if these is any transfer limit.
	// Or is it better to get one based in path?

	paths, err := c.MetakvBigValueList(ScheduleCreateTokenPath)
	if err != nil {
		return nil, err
	}

	var result []*ScheduleCreateToken

	if len(paths) != 0 {
		result = make([]*ScheduleCreateToken, 0, len(paths))
		for _, path := range paths {
			token := &ScheduleCreateToken{}
			exist, err := c.MetakvBigValueGet(path, token)
			if err != nil {
				return nil, err
			}

			if exist {
				result = append(result, token)
			}
		}
	}

	return result, nil
}

func UnmarshallScheduleCreateToken(data []byte) (*ScheduleCreateToken, error) {

	r := new(ScheduleCreateToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func ScheduleCreateTokenExist(defnId c.IndexDefnId) (bool, error) {

	token := &ScheduleCreateToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvBigValueGet(ScheduleCreateTokenPath+id, token)
}

func MarshallScheduleCreateTokenList(tokens *ScheduleCreateTokenList) ([]byte, error) {
	buf, err := json.Marshal(&tokens)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func FetchScheduleCreateToken(path string) (*ScheduleCreateToken, error) {

	token := &ScheduleCreateToken{}
	exists, err := c.MetakvBigValueGet(path, token)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return token, nil
}

func GetScheduleCreateToken(defnId c.IndexDefnId) (*ScheduleCreateToken, error) {
	id := fmt.Sprintf("%v", defnId)
	return FetchScheduleCreateToken(ScheduleCreateTokenPath + id)
}

func GetPrefixFromScheduleCreateTokenPath(path string) (string, error) {

	if len(path) <= len(ScheduleCreateTokenPath) {
		return "", fmt.Errorf("Invalid path %v", path)
	}

	l := len(ScheduleCreateTokenPath)
	id := path[l:]

	if loc := strings.Index(id, "/"); loc != -1 {
		return path[:l+loc], nil
	} else {
		return path, nil
	}
}

func GetScheduleCreateTokenPathFromDefnId(defnId c.IndexDefnId) string {
	return ScheduleCreateTokenPath + fmt.Sprintf("%v", defnId)
}

func UpdateScheduleCreateToken(token *ScheduleCreateToken) error {
	path := fmt.Sprintf("%v", token.Definition.DefnId)

	err := c.MetakvBigValueSet(ScheduleCreateTokenPath+path, token)
	if err != nil {
		return err
	}

	// Log a system event when ownership of background index creation is transferred
	// to a new indexer node.
	idxDefn := token.Definition
	indexerId := token.IndexerId
	event := systemevent.NewDDLSystemEvent("UpdateScheduleCreateIndex", idxDefn.DefnId,
		c.IndexInstId(0), uint64(0), uint64(0), c.IndexInstId(0), string(indexerId), "")

	systemevent.InfoEvent("Indexer", systemevent.EVENTID_INDEX_SCHED_CREATE, event)

	return nil
}

func PostStopScheduleCreateToken(defnId c.IndexDefnId, reason string, ctime int64, indexerId c.IndexerId) error {

	token := &StopScheduleCreateToken{
		Reason: reason,
		Ctime:  ctime,
		DefnId: defnId,
	}

	path := fmt.Sprintf("%v", defnId)
	err := c.MetakvSet(StopScheduleCreateTokenPath+path, token)
	if err != nil {
		return err
	}

	event := systemevent.NewDDLSystemEvent("IndexScheduledCreationError", defnId,
		c.IndexInstId(0), uint64(0), uint64(0), c.IndexInstId(0), string(indexerId), reason)

	systemevent.ErrorEvent("Indexer", systemevent.EVENTID_INDEX_SCHED_CREATE_ERROR, event)

	return nil
}

// ListAllStopScheduleCreateTokens retrieves all stop schedule create tokens
// from metakv into a slice of token pointers. Result is nil if no tokens.
func ListAllStopScheduleCreateTokens() ([]*StopScheduleCreateToken, error) {
	entries, err := c.MetakvList(StopScheduleCreateTokenPath)
	if err != nil {
		return nil, err
	}

	var result []*StopScheduleCreateToken
	if len(entries) != 0 {
		result = make([]*StopScheduleCreateToken, 0, len(entries))
		for _, entry := range entries {
			token := new(StopScheduleCreateToken)
			err = json.Unmarshal(entry.Value, token)
			if err != nil {
				return nil, err
			}
			result = append(result, token)
		}
	}

	return result, nil
}

func UnmarshallStopScheduleCreateToken(data []byte) (*StopScheduleCreateToken, error) {

	r := new(StopScheduleCreateToken)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}

	return r, nil
}

func StopScheduleCreateTokenExist(defnId c.IndexDefnId) (bool, error) {

	token := &StopScheduleCreateToken{}
	id := fmt.Sprintf("%v", defnId)
	return c.MetakvGet(StopScheduleCreateTokenPath+id, token)
}

func GetStopScheduleCreateToken(defnId c.IndexDefnId) (*StopScheduleCreateToken, error) {

	token := &StopScheduleCreateToken{}
	id := fmt.Sprintf("%v", defnId)
	success, err := c.MetakvGet(StopScheduleCreateTokenPath+id, token)
	if err != nil {
		return nil, err
	}

	if !success {
		return nil, nil
	}

	return token, nil
}

func DeleteStopScheduleCreateToken(defnId c.IndexDefnId) error {

	id := fmt.Sprintf("%v", defnId)
	return c.MetakvDel(StopScheduleCreateTokenPath + id)
}

func MarshallStopScheduleCreateTokenList(tokens *StopScheduleCreateTokenList) ([]byte, error) {
	buf, err := json.Marshal(&tokens)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func GetStopScheduleCreateTokenPathFromDefnId(defnId c.IndexDefnId) string {
	return StopScheduleCreateTokenPath + fmt.Sprintf("%v", defnId)
}

//////////////////////////////////////////////////////////////
// CommandListener
//////////////////////////////////////////////////////////////

func NewCommandListener(donech chan bool, doCreate bool, doBuild bool,
	doDelete bool, doDropInst bool, doSchedule bool, doStopSched bool) *CommandListener {

	return &CommandListener{
		doCreate:        doCreate,
		doBuild:         doBuild,
		doDelete:        doDelete,
		doDropInst:      doDropInst,
		doSchedule:      doSchedule,
		doStopSched:     doStopSched,
		createTokens:    make(map[string]*CreateCommandToken),
		buildTokens:     make(map[string]*BuildCommandToken),
		deleteTokens:    make(map[string]*DeleteCommandToken),
		dropInstTokens:  make(map[string]*DropInstanceCommandToken),
		scheduleTokens:  make(map[string]*ScheduleCreateToken),
		stopSchedTokens: make(map[string]*StopScheduleCreateToken),
		schedTokensDel:  make(map[string]bool),
		cancelCh:        make(chan struct{}),
		donech:          donech,
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

func (m *CommandListener) GetNewScheduleCreateTokens() map[string]*ScheduleCreateToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.scheduleTokens) != 0 {
		result := m.scheduleTokens
		m.scheduleTokens = make(map[string]*ScheduleCreateToken)
		return result
	}

	return nil
}

func (m *CommandListener) AddNewScheduleCreateToken(path string, token *ScheduleCreateToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.scheduleTokens[path] = token
}

func (m *CommandListener) GetNewStopScheduleCreateTokens() map[string]*StopScheduleCreateToken {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.stopSchedTokens) != 0 {
		result := m.stopSchedTokens
		m.stopSchedTokens = make(map[string]*StopScheduleCreateToken)
		return result
	}

	return nil
}

func (m *CommandListener) AddNewStopScheduleCreateToken(path string, token *StopScheduleCreateToken) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.stopSchedTokens[path] = token
}

func (m *CommandListener) GetDeletedScheduleCreateTokenPaths() map[string]bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.schedTokensDel) != 0 {
		result := m.schedTokensDel
		m.schedTokensDel = make(map[string]bool)
		return result
	}

	return nil
}

func (m *CommandListener) AddDeletedScheduleCreateTokenNoLock(path string) {
	m.schedTokensDel[path] = true
}

// ListenTokens perpetually listens (in RunObserveChildren) for creation of and changes to DDL
// tokens in metakv. RunObserveChildren will call the metaKVCallback function defined here for
// each such event.
func (m *CommandListener) ListenTokens() {

	metaKVCallback := func(kve metakv.KVEntry) error {
		if strings.Contains(kve.Path, DropInstanceDDLCommandTokenPath) {
			m.handleNewDropInstanceCommandToken(kve.Path, kve.Value)

		} else if strings.Contains(kve.Path, DeleteDDLCommandTokenPath) {
			m.handleNewDeleteCommandToken(kve.Path, kve.Value)

		} else if strings.Contains(kve.Path, BuildDDLCommandTokenPath) {
			m.handleNewBuildCommandToken(kve.Path, kve.Value)

		} else if strings.Contains(kve.Path, CreateDDLCommandTokenPath) {
			m.handleNewCreateCommandToken(kve.Path, kve.Value)

		} else if strings.Contains(kve.Path, ScheduleCreateTokenPath) {
			m.handleNewScheduleCreateToken(kve.Path, kve.Value)

		} else if strings.Contains(kve.Path, StopScheduleCreateTokenPath) {
			m.handleNewStopScheduleCreateToken(kve.Path, kve.Value)
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

func (m *CommandListener) handleNewScheduleCreateToken(path string, value []byte) {

	if !m.doSchedule {
		return
	}

	// Note that in case of Big Value for the token, following steps will be
	// executed multiple times, once for each part. But these steps are idempotent.
	// The behavior is same as CreateCommandToken

	prefix, err := GetPrefixFromScheduleCreateTokenPath(path)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process schedule create token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			m.AddDeletedScheduleCreateTokenNoLock(prefix)
			delete(m.scheduleTokens, path)
		}()

		return
	}

	var token *ScheduleCreateToken
	token, err = FetchScheduleCreateToken(prefix)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process schedule create token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	if token == nil {
		logging.Warnf("CommandListener: Failed to process schedule create token.  Skp %v.  Internal Error = %v.", path, fmt.Errorf("Nil token."))
		return
	}

	m.AddNewScheduleCreateToken(prefix, token)
}

func (m *CommandListener) handleNewStopScheduleCreateToken(path string, value []byte) {

	if !m.doStopSched {
		return
	}

	if value == nil {
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			delete(m.stopSchedTokens, path)
		}()

		return
	}

	token, err := UnmarshallStopScheduleCreateToken(value)
	if err != nil {
		logging.Warnf("CommandListener: Failed to process stop schedule create token.  Skp %v.  Internal Error = %v.", path, err)
		return
	}

	m.AddNewStopScheduleCreateToken(path, token)
}

//
// Generate a token to metakv for enabling PlasmaInMemoryCompression feature
//
func PostEnablePlasmaInMemoryCompressionToken() error {

	commandToken := &PlasmaInMemoryCompresisonToken{
		enabled: true,
	}

	return c.MetakvSet(PlasmaInMemoryCompressionFeaturePath, commandToken)
}

//
// Check PlasmaInMemoryCompression feature is enabled in metakv
//
func EnablePlasmaInMemoryCompressionTokenExist() (bool, error) {

	commandToken := &PlasmaInMemoryCompresisonToken{}
	return c.MetakvGet(PlasmaInMemoryCompressionFeaturePath, commandToken)
}

func CheckInProgressCommandTokensForBucket(bucketName string) (_ bool, inProgDefns []string, err error) {

	// List create tokens
	createCmdTokens, err := ListAndFetchAllCreateCommandTokens()
	if err != nil {
		return false, nil, err
	}

	// Filter creates for bucket
	for _, createCmdToken := range createCmdTokens {
		for _, idxDefns := range createCmdToken.Definitions {
			for _, idxDefn := range idxDefns {
				if idxDefn.Bucket == bucketName {
					inProgDefns = append(inProgDefns, fmt.Sprintf("Create idx[%v]", idxDefn.Name))
				}
			}
		}
	}

	// TODO: Drop index is not billable activity and presence of delete token should handled differently
	// List delete tokens
	deleteCmdTokens, err := ListDeleteCommandToken()
	if err != nil {
		return false, nil, err
	}

	// Filter deletes for bucket
	for _, deleteCmdToken := range deleteCmdTokens {
		if bucketName == deleteCmdToken.Bucket {
			inProgDefns = append(inProgDefns, fmt.Sprintf("Delete DefnId[%v]", deleteCmdToken.DefnId))
		}
	}

	// List drop tokens
	dropCmdTokens, err := ListAndFetchAllDropInstanceCommandToken(1)
	if err != nil {
		return false, nil, err
	}

	// Filter drops for bucket
	for _, dropCmdToken := range dropCmdTokens {
		if bucketName == dropCmdToken.Defn.Bucket {
			inProgDefns = append(inProgDefns, fmt.Sprintf("Drop idx[%v]", dropCmdToken.Defn.Name))
		}
	}

	// List build tokens
	buildCmdTokens, err := ListBuildCommandTokens()
	if err != nil {
		return false, nil, err
	}

	// Filter builds for bucket
	for _, buildCmdToken := range buildCmdTokens {
		if bucketName == buildCmdToken.Bucket {
			inProgDefns = append(inProgDefns, fmt.Sprintf("Build DefnId[%v]", buildCmdToken.DefnId))
		}
	}

	// Check schedule tokens

	schCreateTokens, err := ListAllScheduleCreateTokens()
	if err != nil {
		return false, nil, err
	}

	stopSchCreateTokens, err := ListAllStopScheduleCreateTokens()
	if err != nil {
		return false, nil, err
	}

	stopSchCreateTokensMap := make(map[common.IndexDefnId]bool)
	for _, stopSchCreateToken := range stopSchCreateTokens {
		stopSchCreateTokensMap[stopSchCreateToken.DefnId] = true
	}

	for _, schCreateToken := range schCreateTokens {
		// Filter schedule create tokens for bucket
		if bucketName != schCreateToken.Definition.Bucket {
			continue
		}

		// Match with stop schedule create token
		if _, matchExists := stopSchCreateTokensMap[schCreateToken.Definition.DefnId]; !matchExists {
			inProgDefns = append(inProgDefns, fmt.Sprintf("Sch idx[%v]", schCreateToken.Definition.Name))
		}
	}

	return len(inProgDefns) > 0, inProgDefns, nil
}

func DeleteAllCommandTokens() error {
	return metakv.RecursiveDelete(CommandMetakvDir)
}
