// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseObjutil class - Perform file transfers to and from archive storage, which can be the local
// filesystem or a CSP by using one of the following target path prefixes:
//   o file:// (or no prefix) - local filesystem
//   o s3:// - AWS S3
//   o az:// - Azure cloud storage
//   o gs:// - Google cloud storage
//
// Eventually this will use the tools-common/objstore/objutil/ library for all of these, however
// that library's initial implementation only handles AWS S3, so initially the PauseObjutil class
// will implement support for local filesystem directly.
//
// kjc This class is implementing local FS support first; cloud storage work not yet started.
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseObjutil object holds the state for a session of file transfer activity.
type PauseObjutil struct {
}

// NewPauseObjutil is the constructor for the PauseObjutil class.
func NewPauseObjutil() *PauseObjutil {
	return &PauseObjutil{}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Enums
////////////////////////////////////////////////////////////////////////////////////////////////////

// ArchiveEnum defines types of storage archives for Pause-Resume (following Archive_XXX constants)
type ArchiveEnum int

const (
	Archive_NIL  ArchiveEnum = iota // undefined
	Archive_AZ                      // Azure Cloud Storage
	Archive_FILE                    // local filesystem
	Archive_GS                      // Google Cloud Storage
	Archive_S3                      // AWS S3 bucket
)

// String converter for ArchiveEnum type.
func (this ArchiveEnum) String() string {
	switch this {
	case Archive_NIL:
		return "NilArchive"
	case Archive_AZ:
		return "Azure"
	case Archive_FILE:
		return "File"
	case Archive_GS:
		return "Google"
	case Archive_S3:
		return "S3"
	default:
		return fmt.Sprintf("undefinedArchiveEnum_%v", int(this))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Types
////////////////////////////////////////////////////////////////////////////////////////////////////

// ReadAtSeeker is a composition of the reader/seeker/reader at interfaces.
// kjc Temporarily copied from tools-common/objstore/objutil/upload.go
type ReadAtSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseObjutil APIs
////////////////////////////////////////////////////////////////////////////////////////////////////

// Upload wraps the tools-common/objstore/objutil/upload.go Upload method. It writes a single file
// to archive storage.
//
//	remotePath -- path to target "directory" (with xx:// prefix; no prefix = local FS)
//	fileName -- target file name to write in archiveDir
//	body -- reader for the source data
//
// kjc Eventually this will delegate to objutil.Upload but for now it is local impl of local FS only
func Upload(remotePath, fileName string, body ReadAtSeeker) error {
	const _Upload = "PauseObjutil::Upload:"

	// Verify archive type is supported
	archiveType, archiveDir, err := ArchiveInfoFromRemotePath(remotePath)
	if err != nil {
		logging.Errorf("%v ArchiveInfoFromRemotePath error: %v", _Upload, err)
		return err
	}
	if archiveType != Archive_FILE {
		err = fmt.Errorf("%v Unsupported archive type %v", _Upload, archiveType.String())
		logging.Errorf("%v", err.Error())
		return err
	}

	// Create target directory if needed
	var fileMode os.FileMode = 0700
	err = iowrap.Os_MkdirAll(archiveDir, fileMode)
	if err != nil {
		err = fmt.Errorf("%v Os_MkdirAll(%v, %#o) error: %v", _Upload, archiveDir, fileMode, err)
		logging.Errorf("%v", err.Error())
		return err
	}

	// Create the file (automatically truncates it first if it already exists)
	fullPath := archiveDir + fileName
	fileHandle, err := iowrap.Os_Create(fullPath)
	if err != nil {
		err = fmt.Errorf("%v Os_Create(%v) error: %v", _Upload, fullPath, err)
		logging.Errorf("%v", err.Error())
		return err
	}
	defer iowrap.File_Close(fileHandle)

	// Set desired permission bits on target file
	fileMode = 0600
	err = iowrap.File_Chmod(fileHandle, fileMode)
	if err != nil {
		err = fmt.Errorf("%v File_Chmod(fileHandle, %#o) error: %v", _Upload, fileMode, err)
		logging.Errorf("%v", err.Error())
		return err
	}

	// Write the file
	buffer := make([]byte, 64*1024)
	var bytesRead int            // avoid bytesRead, err := in loop shadowing loop condition err
	for err = nil; err == nil; { // err == io.EOF terminates loop; other errors return from it
		bytesRead, err = iowrap.Io_Read(body, buffer)
		_, err2 := iowrap.File_Write(fileHandle, buffer[:bytesRead]) // process bytesRead before err
		if err != nil && err != io.EOF {
			err = fmt.Errorf("%v Io_Read error: %v", _Upload, err)
			logging.Errorf("%v", err.Error())
			return err
		}
		if err2 != nil {
			err = fmt.Errorf("%v File_Write error: %v", _Upload, err2)
			logging.Errorf("%v", err.Error())
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// General methods and functions
////////////////////////////////////////////////////////////////////////////////////////////////////

// ArchiveInfoFromRemotePath returns the archive type and directory from the remotePath from
// either ns_server or test code, or logs and returns an error if the type is missing or
// unrecognized. The type is determined by the remotePath prefix:
//
//	file:// - local filesystem path; used in tests
//	s3://   - AWS S3 bucket and path; used in production
//
// In all cases this will append a trailing "/" if one is not present. For local filesystem, the
// "file://" prefix will be removed from the returned archiveDir, so it works as a regular path:
//
//	"file://foo/bar" becomes relative path "foo/bar/"
//	"file:///foo/bar" becomes absolute path "/foo/bar/"
func ArchiveInfoFromRemotePath(remotePath string) (
	archiveType ArchiveEnum, archiveDir string, err error) {
	const _ArchiveInfoFromRemotePath = "PauseServiceManager::ArchiveInfoFromRemotePath:"

	const (
		PREFIX_AZ   = "az://"   // Azure Cloud Storage target
		PREFIX_FILE = "file://" // local filesystem target
		PREFIX_GS   = "gs://"   // Google Cloud Storage target
		PREFIX_S3   = "s3://"   // AWS S3 target
	)

	// Check for valid archive type
	hasPrefix := true
	if strings.HasPrefix(remotePath, PREFIX_AZ) {
		archiveType = Archive_AZ
	} else if strings.HasPrefix(remotePath, PREFIX_FILE) {
		archiveType = Archive_FILE
	} else if strings.HasPrefix(remotePath, PREFIX_GS) {
		archiveType = Archive_GS
	} else if strings.HasPrefix(remotePath, PREFIX_S3) {
		archiveType = Archive_S3
	} else { // treat as FILE, which does not require the prefix
		hasPrefix = false
		archiveType = Archive_FILE
	}

	// Ensure there is more than just the archive type prefix
	if (!hasPrefix && len(remotePath) == 0) ||
		hasPrefix && ((archiveType == Archive_AZ && len(remotePath) <= len(PREFIX_AZ)) ||
			(archiveType == Archive_FILE && len(remotePath) <= len(PREFIX_FILE)) ||
			(archiveType == Archive_GS && len(remotePath) <= len(PREFIX_GS)) ||
			(archiveType == Archive_S3 && len(remotePath) <= len(PREFIX_S3))) {
		err = fmt.Errorf("%v Missing path body in remotePath '%v'", _ArchiveInfoFromRemotePath, remotePath)
		logging.Errorf(err.Error())
		return Archive_NIL, "", err
	}

	// Ensure there is a trailing slash
	if strings.HasSuffix(remotePath, "/") {
		archiveDir = remotePath
	} else {
		archiveDir = remotePath + "/"
	}

	// For Archive_FILE, strip off "file://" prefix
	if hasPrefix && archiveType == Archive_FILE {
		archiveDir = strings.Replace(archiveDir, PREFIX_FILE, "", 1)
	}

	return archiveType, archiveDir, nil
}

// ********* CLONE of COPIER obj in Plasma***************
type Copier interface {
	// upload
	GetCopyRoot() string // destination directory or plasma prefix for s3
	CopyFile(ctx context.Context, src, dst string, off, sz int64) (int64, error)
	CopyBytes(ctx context.Context, bs []byte, src, dst string) (int64, error)
	UploadBytes(ctx context.Context, bs []byte, dst string) (int64, error)

	// download
	GetRestoreRoot() string // staging
	InitRestore(src string) error
	RestoreFile(ctx context.Context, dst, src string) (int64, error)
	RestoreFiles(ctx context.Context, dst, src string) (int64, error)
	DownloadBytes(ctx context.Context, src string) ([]byte, error)

	// delete
	CleanupFiles(string) error

	// encoding
	GetPathEncoding(string) (string, error)

	// control
	CancelCopy()
	IsCancelled() bool
	Debug() bool
}
