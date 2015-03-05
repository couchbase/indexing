package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <libforestdb/forestdb.h>
import "C"

const (
	RESULT_SUCCESS                 C.fdb_status = 0
	RESULT_INVALID_ARGS            Error        = -1
	RESULT_OPEN_FAIL               Error        = -2
	RESULT_NO_SUCH_FILE            Error        = -3
	RESULT_WRITE_FAIL              Error        = -4
	RESULT_READ_FAIL               Error        = -5
	RESULT_CLOSE_FAIL              Error        = -6
	RESULT_COMMIT_FAIL             Error        = -7
	RESULT_ALLOC_FAIL              Error        = -8
	RESULT_KEY_NOT_FOUND           Error        = -9
	RESULT_RONLY_VIOLATION         Error        = -10
	RESULT_COMPACTION_FAIL         Error        = -11
	RESULT_ITERATOR_FAIL           Error        = -12
	RESULT_SEEK_FAIL               Error        = -13
	RESULT_FSYNC_FAIL              Error        = -14
	RESULT_CHECKSUM_ERROR          Error        = -15
	RESULT_FILE_CORRUPTION         Error        = -16
	RESULT_COMPRESSION_FAIL        Error        = -17
	RESULT_NO_DB_INSTANCE          Error        = -18
	RESULT_FAIL_BY_ROLLBACK        Error        = -19
	RESULT_INVALID_CONFIG          Error        = -20
	RESULT_MANUAL_COMPACTION_FAIL  Error        = -21
	RESULT_INVALID_COMPACTION_MODE Error        = -22
	RESULT_FILE_IS_BUSY            Error        = -23
	RESULT_FILE_REMOVE_FAIL        Error        = -24
	RESULT_FILE_RENAME_FAIL        Error        = -25
	RESULT_TRANSACTION_FAIL        Error        = -26
	RESULT_FAIL                    Error        = -100
)

type Error int

func (e Error) Error() string {
	return resultMessages[int(e)]
}

var resultMessages = map[int]string{
	0:    "success",
	-1:   "invalid args",
	-2:   "open fail",
	-3:   "no such file",
	-4:   "write fail",
	-5:   "read fail",
	-6:   "close fail",
	-7:   "commit fail",
	-8:   "alloc fail",
	-9:   "key not found",
	-10:  "read-only violation",
	-11:  "compaction fail",
	-12:  "iterator fail",
	-13:  "seek fail",
	-14:  "fsync fail",
	-15:  "checksum error",
	-16:  "file corruption",
	-17:  "compression fail",
	-18:  "no db instance",
	-19:  "fail by rollback",
	-20:  "invalid config",
	-21:  "manual compaction fail",
	-22:  "invalid compaction mode",
	-23:  "file is busy",
	-24:  "file remove fail",
	-25:  "file rename fail",
	-26:  "transaction fail",
	-100: "fail",
}
