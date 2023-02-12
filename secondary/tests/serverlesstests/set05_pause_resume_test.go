package serverlesstests

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/couchbase/indexing/secondary/manager"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

const (
	pauseResumeBucket     string = "pauseResumeBucket" // bucket to create indexes in
	pauseResumeScope      string = "indexing"
	pauseResumeCollection string = "test"
)

// Proxies for some constants from pause_service_manager.go as tests cannot import the indexer
// package as that pulls in sigar which won't build here. (Thus these are also not exported.)
const (
	archive_FILE = "File"
)

// getIndexerNodeAddrs returns ipAddr:port of each index node.
func getIndexerNodeAddrs(t *testing.T) []string {
	const _getIndexerNodeAddrs = "set05_pause_resume_test::getIndexerNodeAddrs:"

	indexerNodeAddrs, err := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	FailTestIfError(err,
		fmt.Sprintf("%v GetIndexerNodesHttpAddresses returned error", _getIndexerNodeAddrs), t)
	if len(indexerNodeAddrs) == 0 {
		t.Fatalf("%v No Index nodes found", _getIndexerNodeAddrs)
	}
	return indexerNodeAddrs
}

// skipTest determines whether the calling test should be run based on whether
// MultipleIndexerTests is enabled and the original cluster config file has at
// least 4 nodes in it, for backward compability with legacy test runs with
// fewer nodes defined (so these tests get skipped instead of failing).
func skipTest() bool {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return true
	}
	return false
}

// getTaskList calls the generic generic_service_manager.go GetTaskList cbauth RPC API on the
// specified nodeAddr (ipAddr:port) and returns its TaskList response. It omits nil responses so as
// not to mask mysterious failures by making it look like a non-trivial response was received.
func getTaskList(nodeAddr string, t *testing.T) (taskList *service.TaskList) {
	const _getTaskList = "set05_pause_resume_test::getTaskList:"

	url := makeUrlForIndexNode(nodeAddr, "/test/GetTaskList")
	resp, err := http.Get(url)
	FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _getTaskList, url), t)
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _getTaskList, url), t)

	var getTaskListResponse tc.GetTaskListResponse
	err = json.Unmarshal(respBody, &getTaskListResponse)
	FailTestIfError(err, fmt.Sprintf("%v json.Unmarshal %v returned error", _getTaskList, url), t)

	// Return value
	taskList = getTaskListResponse.TaskList

	// For debugging: Print the response
	log.Printf("%v getTaskListResponse: %+v, TaskList %+v", _getTaskList,
		getTaskListResponse, taskList)

	return taskList
}

// getTaskListAll calls the generic generic_service_manager.go GetTaskList cbauth RPC API on all
// Indexer nodes and returns their TaskList responses. It omits nil responses so as not to mask
// mysterious failures by making it look like a non-trivial response was received.
func getTaskListAll(t *testing.T) (taskLists []*service.TaskList) {
	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		taskList := getTaskList(nodeAddr, t)
		if taskList != nil {
			taskLists = append(taskLists, taskList)
		}
	}
	return taskLists
}

// cancelTask calls the generic_sercvice_manager.go CancelTaskList cbauth RPC API on each index node
// with the provided arguments.
func cancelTask(id string, rev uint64, t *testing.T) {
	const _cancelTask = "set05_pause_resume_test::cancelTask:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/CancelTask?id=%v&rev=%v", id, rev)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _cancelTask, url), t)

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _cancelTask,
			url), t)
	}
}

// preparePause calls the pause_service_manager.go PreparePause cbauth RPC API on each
// index node with the provided arguments.
func preparePause(id, bucket, remotePath string, t *testing.T) {
	const _preparePause = "set05_pause_resume_test::preparePause:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/PreparePause?id=%v&bucket=%v&remotePath=%v",
			id, bucket, remotePath)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _preparePause, url), t)
		defer resp.Body.Close()

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _preparePause,
			url), t)
	}
}

// pause performs the Elixir Pause action (hibernate a bucket) using local disk instead of S3 by
// calling the pause_service_manager.go Pause cbauth RPC API on only one Index node, which becomes
// the master. It returns the address of the master.
func pause(id, bucket, remotePath string, t *testing.T) (masterAddr string) {
	const _pause = "set05_pause_resume_test::pause:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	masterAddr = indexerNodeAddrs[0]
	restPath := fmt.Sprintf(
		"/test/Pause?id=%v&bucket=%v&remotePath=%v",
		id, bucket, remotePath)
	url := makeUrlForIndexNode(masterAddr, restPath)
	resp, err := http.Get(url)
	FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _pause, url), t)
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _pause, url), t)

	return masterAddr
}

// prepareResume calls the pause_service_manager.go PrepareResume cbauth RPC API on each
// index node with the provided arguments.
func prepareResume(id, bucket, remotePath string, dryRun bool, t *testing.T) {
	const _prepareResume = "set05_pause_resume_test::prepareResume:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/PrepareResume?id=%v&bucket=%v&remotePath=%v&dryRun=%v",
			id, bucket, remotePath, dryRun)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _prepareResume, url), t)
		defer resp.Body.Close()

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _prepareResume,
			url), t)
	}
}

// resume performs the Elixir Resume action (unhibernate a bucket) using local disk instead of S3 by
// calling the pause_service_manager.go Resume cbauth RPC API on only one Index node, which becomes
// the master. It returns the address of the master.
func resume(id, bucket, remotePath string, dryRun bool, t *testing.T) (masterAddr string) {
	const _resume = "set05_pause_resume_test::resume:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	masterAddr = indexerNodeAddrs[0]
	restPath := fmt.Sprintf(
		"/test/Resume?id=%v&bucket=%v&remotePath=%v&dryRun=%v",
		id, bucket, remotePath, dryRun)
	url := makeUrlForIndexNode(masterAddr, restPath)
	resp, err := http.Get(url)
	FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _resume, url), t)
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _resume, url), t)

	return masterAddr
}

func runPause(t *testing.T, numIndexNodes int, pauseTaskId, remotePath, archivePath string,
	rev uint64) *indexer.PauseMetadata {
	var pauseMetadata *indexer.PauseMetadata
	t.Run("PreparePause", func(t *testing.T) {
		const _TestPreparePause = "set05_pause_resume_test::_TestPreparePause:"

		log.Printf("%v Before start, calling GetTaskList", _TestPreparePause)
		taskLists := getTaskListAll(t)
		if len(taskLists) != numIndexNodes {
			t.Fatalf("%v Before start expected %v getTaskListAll replies, got %v", _TestPreparePause, numIndexNodes, len(taskLists))
		}

		// PreparePause
		log.Printf("%v Calling PreparePause", _TestPreparePause)
		preparePause(pauseTaskId, pauseResumeBucket, remotePath, t)
		log.Printf("%v Calling GetTaskList(PreparePause)", _TestPreparePause)
		taskLists = getTaskListAll(t)
		if len(taskLists) != numIndexNodes {
			t.Fatalf("%v After PreparePause expected %v getTaskListAll replies, got %v", _TestPreparePause,
				numIndexNodes, len(taskLists))
		}
		numNodeTasks := 1 // number of tasks expected in each node's task list
		for _, taskList := range taskLists {
			if len(taskList.Tasks) != numNodeTasks {
				t.Fatalf("%v PreparePause expected len(taskList.Tasks) = %v, got %v", _TestPreparePause,
					numNodeTasks, len(taskList.Tasks))
			}
			task := taskList.Tasks[0]
			if task.ID != pauseTaskId {
				t.Fatalf("%v PreparePause expected task.ID '%v', got '%v'", _TestPreparePause,
					pauseTaskId, task.ID)
			}
			if task.Status != service.TaskStatusRunning {
				t.Fatalf("%v PreparePause expected task.Status '%v', got '%v'", _TestPreparePause,
					service.TaskStatusRunning, task.Status)
			}
			if task.Type != service.TaskTypePrepared {
				t.Fatalf("%v PreparePause expected task.Type '%v', got '%v'", _TestPreparePause,
					service.TaskTypePrepared, task.Type)
			}
			val := task.Extra["bucket"]
			if val != pauseResumeBucket {
				t.Fatalf("%v PreparePause expected bucket '%v', got '%v'", _TestPreparePause,
					pauseResumeBucket, val)
			}
			val = task.Extra["archivePath"]
			if val != archivePath {
				t.Fatalf("%v PreparePause expected archivePath '%v', got '%v'", _TestPreparePause,
					archivePath, val)
			}
			val = task.Extra["archiveType"]
			if val != archive_FILE {
				t.Fatalf("%v PreparePause expected archiveType '%v', got '%v'", _TestPreparePause,
					archive_FILE, val)
			}
			val = task.Extra["master"]
			if val != false {
				t.Fatalf("%v PreparePause expected master '%v', got '%v'", _TestPreparePause,
					false, val)
			}
		}
	})

	t.Run("Pause", func(t *testing.T) {
		const _TestPause = "set05_pause_resume_test.go::TestPause:"

		secondaryindex.ChangeIndexerSettings("indexer.pause_resume.compression", false, clusterconfig.Username, clusterconfig.Password, kvaddress)

		log.Printf("%v Before start, calling GetTaskList", _TestPause)
		taskLists := getTaskListAll(t)
		if len(taskLists) != numIndexNodes {
			t.Fatalf("%v Before start expected %v getTaskListAll replies, got %v", _TestPause,
				numIndexNodes, len(taskLists))
		}

		// Pause
		log.Printf("%v Calling Pause", _TestPause)
		masterAddr := pause(pauseTaskId, pauseResumeBucket, remotePath, t)
		start := time.Now().Unix()
		log.Printf("%v Calling GetTaskList(Pause) on masterAddr: %v", _TestPause, masterAddr)
		taskList := getTaskList(masterAddr, t)
		if taskList == nil {
			t.Fatalf("%v After Pause expected master's TaskList, got nil", _TestPause)
		}
		numNodeTasks := 2 // number of tasks expected in master node's task list
		if len(taskList.Tasks) != numNodeTasks {
			t.Fatalf("%v Pause expected len(taskList.Tasks) = %v, got %v", _TestPause,
				numNodeTasks, len(taskList.Tasks))
		}
		task := taskList.Tasks[numNodeTasks-1]
		if task.ID != pauseTaskId {
			t.Fatalf("%v Pause expected task.ID '%v', got '%v'", _TestPause, pauseTaskId, task.ID)
		}
		if task.Status != service.TaskStatusRunning {
			t.Fatalf("%v Pause expected task.Status '%v', got '%v'", _TestPause,
				service.TaskStatusRunning, task.Status)
		}
		if task.Type != service.TaskTypeBucketPause {
			t.Fatalf("%v Pause expected task.Type '%v', got '%v'", _TestPause,
				service.TaskTypeBucketResume, task.Type)
		}
		val := task.Extra["bucket"]
		if val != pauseResumeBucket {
			t.Fatalf("%v Pause expected bucket '%v', got '%v'", _TestPause, pauseResumeBucket, val)
		}
		val = task.Extra["archivePath"]
		if val != archivePath {
			t.Fatalf("%v Pause expected archivePath '%v', got '%v'", _TestPause, archivePath, val)
		}
		val = task.Extra["archiveType"]
		if val != archive_FILE {
			t.Fatalf("%v Pause expected archiveType '%v', got '%v'", _TestPause,
				archive_FILE, val)
		}
		val = task.Extra["master"]
		if val != true {
			t.Fatalf("%v Pause expected master '%v', got '%v'", _TestPause, true, val)
		}

		for list := getTaskList(masterAddr, t); list != nil && len(list.Tasks) != 0; list = getTaskList(masterAddr, t) {
			errStr := new(strings.Builder)
			for _, task := range list.Tasks {
				if len(task.ErrorMessage) != 0 {
					errStr.WriteString(task.ErrorMessage)
				}
			}
			if errStr.Len() != 0 {
				t.Fatalf("%v Got error in executing pause err: %v", _TestPause, errStr.String())
			}
			log.Printf("Waiting for pause to finish since %v seconds. Current status of list: %v...", time.Now().Unix()-start, list)
			time.Sleep(5 * time.Second)
		}

		// Read and verify /tmp/TestPause/pauseMetadata.json
		filePath := archivePath + indexer.FILENAME_PAUSE_METADATA
		metadataJson, err := tc.ReadFileToString(filePath)
		if err != nil {
			t.Fatalf("%v Pause ReadFileToString(%v) returned error: %v", _TestPause, filePath, err)
		}
		metadataBytes, err := common.ChecksumAndUncompress([]byte(metadataJson))
		if err != nil {
			t.Fatalf("%v Pause ChecksumAndUncompress returned error: %v", _TestPause, err)
		}
		pauseMetadata = new(indexer.PauseMetadata)
		if err = json.Unmarshal(metadataBytes, pauseMetadata); err != nil {
			t.Fatalf("%v couldn't unmarshal saved data to type; err: %v", _TestPause, err)
		}
		log.Printf("%v read pause metadata as: %v", _TestPause, pauseMetadata)
		expectedJson := common.GetLocalInternalVersion()
		if !expectedJson.Equals(common.InternalVersion(pauseMetadata.Version)) {
			t.Fatalf("%v Pause expected version '%v', got '%v'", _TestPause, expectedJson,
				pauseMetadata.Version)
		}
		if len(pauseMetadata.Data) != 2 {
			t.Fatalf("%v coudln't pause on all nodes. expected %v, got %v", _TestPause, 2, len(pauseMetadata.Data))
		}

		secondaryindex.ChangeIndexerSettings("indexer.pause_resume.compression", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	})

	t.Run("CancelPause", func(t *testing.T) {
		const _TestPreparePauseAndPrepareResume = "set05_pause_resume_test::CancelPause:"
		// CancelTask
		log.Printf("%v Calling CancelTask", _TestPreparePauseAndPrepareResume)
		cancelTask(pauseTaskId, rev, t)
		log.Printf("%v Calling GetTaskList()", _TestPreparePauseAndPrepareResume)
		taskLists := getTaskListAll(t)
		if len(taskLists) != numIndexNodes {
			t.Fatalf("%v After CancelTask expected %v getTaskListAll replies, got %v", _TestPreparePauseAndPrepareResume,
				numIndexNodes, len(taskLists))
		}
		numNodeTasks := 0 // number of tasks expected in each node's task list
		for _, taskList := range taskLists {
			if len(taskList.Tasks) != numNodeTasks {
				t.Fatalf("%v CancelTask expected len(taskList.Tasks) = %v, got %v", _TestPreparePauseAndPrepareResume,
					numNodeTasks, len(taskList.Tasks))
			}
		}
	})

	return pauseMetadata
}

func runResume(t *testing.T, numIndexNodes int, resumeTaskId, remotePath, archivePath string,
	rev uint64) {
	runs := []bool{true, false}

	for _, dryRun := range runs {
		t.Run(fmt.Sprintf("PrepareResume/dryRun-%v", dryRun), func(t *testing.T) {
			const _TestPrepareResume = "set05_pause_resume_test::_TestPrepareResume:"
			if skipTest() {
				log.Printf("%v Test skipped", _TestPrepareResume)
				return
			}

			log.Printf("%v Before start, calling GetTaskList", _TestPrepareResume)
			taskLists := getTaskListAll(t)
			if len(taskLists) != numIndexNodes {
				t.Fatalf("%v Before start expected %v getTaskListAll replies, got %v", _TestPrepareResume, numIndexNodes, len(taskLists))
			}

			log.Printf("%v Calling PrepareResume", _TestPrepareResume)
			prepareResume(resumeTaskId, pauseResumeBucket, remotePath, dryRun, t)
			log.Printf("%v Calling GetTaskList(PrepareResume)", _TestPrepareResume)
			taskLists = getTaskListAll(t)
			if len(taskLists) != numIndexNodes {
				t.Fatalf("%v After PrepareResume expected %v getTaskListAll replies, got %v", _TestPrepareResume,
					numIndexNodes, len(taskLists))
			}
			numNodeTasks := 1 // number of tasks expected in each node's task list
			for _, taskList := range taskLists {
				if len(taskList.Tasks) != numNodeTasks {
					t.Fatalf("%v PrepareResume expected len(taskList.Tasks) = %v, got %v", _TestPrepareResume,
						numNodeTasks, len(taskList.Tasks))
				}
				task := taskList.Tasks[0]
				if task.ID != resumeTaskId {
					t.Fatalf("%v PrepareResume expected task.ID '%v', got '%v'", _TestPrepareResume,
						resumeTaskId, task.ID)
				}
				if task.Status != service.TaskStatusRunning {
					t.Fatalf("%v PrepareResume expected task.Status '%v', got '%v'", _TestPrepareResume,
						service.TaskStatusRunning, task.Status)
				}
				if task.Type != service.TaskTypePrepared {
					t.Fatalf("%v PrepareResume expected task.Type '%v', got '%v'", _TestPrepareResume,
						service.TaskTypePrepared, task.Type)
				}
				val := task.Extra["bucket"]
				if val != pauseResumeBucket {
					t.Fatalf("%v PrepareResume expected bucket '%v', got '%v'", _TestPrepareResume,
						pauseResumeBucket, val)
				}
				val = task.Extra["dryRun"]
				if val != dryRun {
					t.Fatalf("%v PrepareResume expected dryRun '%v', got '%v'", _TestPrepareResume,
						dryRun, val)
				}
				val = task.Extra["archivePath"]
				if val != archivePath {
					t.Fatalf("%v PrepareResume expected archivePath '%v', got '%v'", _TestPrepareResume,
						archivePath, val)
				}
				val = task.Extra["archiveType"]
				if val != archive_FILE {
					t.Fatalf("%v PrepareResume expected archiveType '%v', got '%v'", _TestPrepareResume,
						archive_FILE, val)
				}
				val = task.Extra["master"]
				if val != false {
					t.Fatalf("%v PrepareResume expected master '%v', got '%v'", _TestPrepareResume,
						false, val)
				}
			}
		})

		t.Run(fmt.Sprintf("Resume/dryRun-%v", dryRun), func(t *testing.T) {
			const _TestResume = "set05_pause_resume_test.go::TestResume:"

			log.Printf("%v Before start, calling GetTaskList", _TestResume)
			taskLists := getTaskListAll(t)
			if len(taskLists) != numIndexNodes {
				t.Fatalf("%v Before start expected %v getTaskListAll replies, got %v", _TestResume,
					numIndexNodes, len(taskLists))
			}

			// Resume
			log.Printf("%v Calling Resume", _TestResume)
			masterAddr := resume(resumeTaskId, pauseResumeBucket, remotePath, dryRun, t)
			start := time.Now().Unix()
			log.Printf("%v Calling GetTaskList(Resume) on masterAddr: %v", _TestResume, masterAddr)
			taskList := getTaskList(masterAddr, t)
			if taskList == nil {
				t.Fatalf("%v After Resume expected master's TaskList, got nil", _TestResume)
			}
			numNodeTasks := 2 // number of tasks expected in master node's task list
			if len(taskList.Tasks) != numNodeTasks {
				t.Fatalf("%v Resume expected len(taskList.Tasks) = %v, got %v", _TestResume,
					numNodeTasks, len(taskList.Tasks))
			}
			task := taskList.Tasks[numNodeTasks-1]
			if task.ID != resumeTaskId {
				t.Fatalf("%v Resume expected task.ID '%v', got '%v'", _TestResume, resumeTaskId, task.ID)
			}
			if task.Status != service.TaskStatusRunning {
				t.Fatalf("%v Resume expected task.Status '%v', got '%v'", _TestResume,
					service.TaskStatusRunning, task.Status)
			}
			if task.Type != service.TaskTypeBucketResume {
				t.Fatalf("%v Resume expected task.Type '%v', got '%v'", _TestResume,
					service.TaskTypeBucketResume, task.Type)
			}
			val := task.Extra["bucket"]
			if val != pauseResumeBucket {
				t.Fatalf("%v Resume expected bucket '%v', got '%v'", _TestResume, pauseResumeBucket, val)
			}
			val = task.Extra["archivePath"]
			if val != archivePath {
				t.Fatalf("%v Resume expected archivePath '%v', got '%v'", _TestResume, archivePath, val)
			}
			val = task.Extra["archiveType"]
			if val != archive_FILE {
				t.Fatalf("%v Resume expected archiveType '%v', got '%v'", _TestResume,
					archive_FILE, val)
			}
			val = task.Extra["master"]
			if val != true {
				t.Fatalf("%v Resume expected master '%v', got '%v'", _TestResume, true, val)
			}

			for list := getTaskList(masterAddr, t); list != nil && len(list.Tasks) != 0; list = getTaskList(masterAddr, t) {
				errStr := new(strings.Builder)
				for _, task := range list.Tasks {
					if len(task.ErrorMessage) != 0 {
						errStr.WriteString(task.ErrorMessage)
					}
				}
				if errStr.Len() != 0 {
					t.Fatalf("%v Got error in executing Resume err: %v", _TestResume, errStr.String())
				}
				log.Printf("Waiting for Resume to finish since %v seconds. Current status of list: %v...", time.Now().Unix()-start, list)
				time.Sleep(5 * time.Second)
			}

			// TODO: add post - resume checks? getLocalIndexMetadata, stats, etc
		})

		t.Run(fmt.Sprintf("CancelResume/dryRun-%v", dryRun), func(t *testing.T) {
			const _TestCancelResume = "set05_pause_resume_test::CancelResume:"
			log.Printf("%v Calling CancelTask", _TestCancelResume)
			cancelTask(resumeTaskId, rev, t)
			log.Printf("%v Calling GetTaskList()", _TestCancelResume)
			taskLists := getTaskListAll(t)
			if len(taskLists) != numIndexNodes {
				t.Fatalf("%v After CancelTask expected %v getTaskListAll replies, got %v", _TestCancelResume,
					numIndexNodes, len(taskLists))
			}
			numNodeTasks := 0 // number of tasks expected in each node's task list
			for _, taskList := range taskLists {
				if len(taskList.Tasks) != numNodeTasks {
					t.Fatalf("%v CancelTask expected len(taskList.Tasks) = %v, got %v", _TestCancelResume,
						numNodeTasks, len(taskList.Tasks))
				}
			}
		})

		t.Run(fmt.Sprintf("TokenCleanup/dryRun-%v", dryRun), func(t *testing.T) {
			common.MetakvRecurciveDel(common.PauseMetakvDir)
		})
	}
}

func comparePauseMetadata(pauseMetadata *indexer.PauseMetadata,
	idxsLocalMetadata []manager.LocalIndexMetadata) bool {
	getShards := func(idxLocalMetadata manager.LocalIndexMetadata) []common.ShardId {
		uniqueShardIds := make(map[common.ShardId]bool)
		for _, topology := range idxLocalMetadata.IndexTopologies {
			for _, indexDefn := range topology.Definitions {
				for _, instance := range indexDefn.Instances {
					for _, partition := range instance.Partitions {
						for _, shard := range partition.ShardIds {
							uniqueShardIds[shard] = true
						}
					}
				}
			}
		}

		shardIds := make([]common.ShardId, 0, len(uniqueShardIds))
		for shardId := range uniqueShardIds {
			shardIds = append(shardIds, shardId)
		}

		return shardIds
	}
	shardIds := make([]common.ShardId, 0)
	for _, idxLocalMetadata := range idxsLocalMetadata {
		shardIds = append(shardIds, getShards(idxLocalMetadata)...)
	}
	found := true
	for i := 0; i < len(shardIds) && found; i++ {
		localFound := false
		for _, shardMap := range pauseMetadata.Data {
			if _, ok := shardMap[shardIds[i]]; ok {
				localFound = true
				break
			}
		}
		found = found && localFound
	}
	totalPausedShards := 0
	for _, shardMap := range pauseMetadata.Data {
		for _, _ = range shardMap {
			totalPausedShards++
		}
	}
	log.Printf("found: %v? lens: %v>=%v", found, len(shardIds), totalPausedShards)
	return found && len(shardIds) >= totalPausedShards
}

func TestPauseResume(rootT *testing.T) {
	var (
		archivePath = rootT.TempDir() + "/"
		remotePath  = fmt.Sprintf("file://%v", archivePath)
		indexName   = "index_eyeColor"
		numDocs     = 100000
	)

	createBucket := func() {
		fmt.Println("=== SETUP   CreateBucket")
		kvutility.CreateBucket(pauseResumeBucket, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512", "")

		kvutility.WaitForBucketCreation(pauseResumeBucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0]})

		manifest := kvutility.CreateCollection(pauseResumeBucket, pauseResumeScope, pauseResumeCollection, clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress)
		cid := kvutility.WaitForCollectionCreation(pauseResumeBucket, pauseResumeScope, pauseResumeCollection, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0]}, manifest)

		CreateDocsForCollection(pauseResumeBucket, cid, numDocs)
	}

	createAndScanIndex := func() {

		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(company)", indexName, pauseResumeBucket, pauseResumeScope, pauseResumeCollection)
		execN1qlAndWaitForStatus(n1qlStatement, pauseResumeBucket, pauseResumeScope, pauseResumeCollection, indexName, "Ready", rootT)

		waitForStatsUpdate()

		// Scan the index
		scanIndexReplicas(indexName, pauseResumeBucket, pauseResumeScope, pauseResumeCollection, []int{0, 1}, numScans, numDocs, 1, rootT)
	}

	deleteBucket := func() {

		fmt.Println("=== CLEANUP   DeleteBucket")
		err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
		tc.HandleError(err, "Error in DropAllSecondaryIndexes on PauseResume bucket")

		kvutility.EnableBucketFlush(pauseResumeBucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.FlushBucket(pauseResumeBucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

		kvutility.DeleteBucket(pauseResumeBucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

		log.Printf("Deleted %v bucket with indexes from cluster", pauseResumeBucket)

	}

	tuneCluster := func() {

		fmt.Println("=== SETUP   TestPauseResume")
		err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
		tc.HandleError(err, "DropAllSecondaryIndexes in pause resume setup")
		log.Printf("Setting up cluster with sample data")
		ok, err := clusterutility.ServerGroupExists(kvaddress, clusterconfig.Username, clusterconfig.Password, "Group 1")
		tc.HandleError(err, "Fetch group info")
		if !ok {
			err = clusterutility.AddServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, "Group 1")
			log.Printf("Added server group 1")
		}

		ok, err = clusterutility.ServerGroupExists(kvaddress, clusterconfig.Username, clusterconfig.Password, "Group 2")
		tc.HandleError(err, "Fetch group info")
		if !ok {
			err = clusterutility.AddServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, "Group 2")
			log.Printf("Added server group 2")
		}
		tc.HandleError(err, "Create Server group 2")
		resetCluster(rootT)

	}

	setup := func() {

		tuneCluster()

		createBucket()

		createAndScanIndex()

		log.Printf("Created keyspace %v.%v.%v with %v docs and %v index", pauseResumeBucket, pauseResumeScope, pauseResumeCollection, numDocs, indexName)
	}

	destroy := func() {
		fmt.Println("=== Cleanup   TestPauseResume")

		var dirTree strings.Builder
		dirTree.WriteString("->" + archivePath + "\n")
		filepath.WalkDir(archivePath, func(path string, d fs.DirEntry, err error) error {
			if path == archivePath {
				return nil
			}
			n := len(strings.Split(path, string(filepath.Separator)))
			dirTree.WriteString(fmt.Sprintf("%*v%v\n", n*2-1, "->", strings.TrimPrefix(path, archivePath)))
			return nil
		})
		log.Printf("Final structure of archivePath: \n%v", dirTree.String())

		deleteBucket()
	}

	setup()
	defer destroy()

	const (
		pauseTaskId   = "pauseTaskId"
		resumeTaskId  = "resumeTaskId"
		rev           = uint64(0) // rev for CancelTask; so far does not matter for Pause-Resume
		numIndexNodes = 2
	)

	pauseMetadata := runPause(rootT, numIndexNodes, pauseTaskId, remotePath, archivePath, rev)

	fmt.Println("=== Cleanup   RemoveIndexes")
	err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(err, "DropAllSecondaryIndexes in pause resume setup")
	// sleeping for DDLs to finish
	time.Sleep(5*time.Minute + 1*time.Second)
	tc.HandleError(common.MetakvRecurciveDel(mc.DeleteDDLCommandTokenPath), "Couldn't delete DeleteDDLCommandTokens")

	fmt.Println("=== RUN   ResumeOnSameCluster")

	// resume on same cluster
	runResume(rootT, numIndexNodes, resumeTaskId, remotePath, archivePath, rev)

	rootT.Run("PostResumeChecks", func(t *testing.T) {

		indexMetadataPath := fmt.Sprintf("/getLocalIndexMetadata?useETag=false&bucket=%v", pauseResumeBucket)
		idxsMetadata := make([]manager.LocalIndexMetadata, 0, 2)

		for _, host := range getIndexerNodeAddrs(rootT) {
			var idxerLocalMetadata manager.LocalIndexMetadata
			url := makeUrlForIndexNode(host, indexMetadataPath)
			resp, err := http.Get(url)
			tc.HandleError(err, "failed to fetch local index metadata for "+host)
			body, err := ioutil.ReadAll(resp.Body)
			tc.HandleError(err, "Read response body")
			log.Printf("Got Local Metadata from %v as -> %v", host, string(body))
			tc.HandleError(json.Unmarshal(body, &idxerLocalMetadata), "failed to unmarshal body for "+host)
			resp.Body.Close()

			idxsMetadata = append(idxsMetadata, idxerLocalMetadata)
		}

		metadataMatches := comparePauseMetadata(pauseMetadata, idxsMetadata)
		if !metadataMatches {
			t.Fatalf("Mismatch in paused metadata and recovered metadata.\nPause Metadata: %v\nNew Index Metadata: %v", pauseMetadata, idxsMetadata)
		}

	})

}
