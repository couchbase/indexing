// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package iowrap

import (
	"bytes"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Routines in this file should be used to wrap all GSI disk I/O calls instead of using Go-native
// calls directly, because these wrappers count errors indicating disk problems, which are used by
// the Autofailover feature. Each wrapper in this file
// o Is named "Package_Function" for the Go-native package.Function it wraps, e.g.
//   Ioutil_ReadAll wraps Go's ioutil.ReadAll FUNCTION,
// o Or is named "Class_Function" for the Go-native class.Method it wraps, e.g.
//   File_Chmod wraps Go's os.File.Chmod METHOD. The receiver is passed as a "this" ptr first arg.
// o Takes the same arguments and returns the same results as the Go function it wraps
//
// Wrappers have only been implemented for disk I/O functions GSI actually uses. Add more wrappers
// as needed following the existing implementation pattern if one you need to call is not here yet.

// diskFailures is really a *uint64 and counts I/O errors that probably indicate a disk hardware
// problem or disk full.
var diskFailures unsafe.Pointer

// init runs at initial package load and creates the uint64 diskFailures points to.
func init() {
	var df uint64 = 0
	diskFailures = (unsafe.Pointer)(&df)
}

// GetDiskFailures returns the value of the diskFailures global variable. Thread-safe.
func GetDiskFailures() uint64 {
	return atomic.LoadUint64((*uint64)(diskFailures))
}

// countDiskFailures atomically increments the diskFailures global if input err's message is
// anything other than that of EINTR. This matches Data Service Autofailover disk failure tracking.
func countDiskFailures(err error) {
	if !strings.Contains(err.Error(), syscall.EINTR.Error()) {
		atomic.AddUint64((*uint64)(diskFailures), 1)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// bytes.Buffer METHOD wrappers -- add more as needed
////////////////////////////////////////////////////////////////////////////////////////////////////

// Buffer_ReadFrom wraps Go-native FUNCTION bytes.Buffer.ReadFrom for disk failure tracking.
func Buffer_ReadFrom(this *bytes.Buffer, r io.Reader) (n int64, err error) {
	n, err = this.ReadFrom(r)
	if err != nil {
		countDiskFailures(err)
	}
	return n, err
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// os.File METHOD wrappers -- add more as needed
////////////////////////////////////////////////////////////////////////////////////////////////////

// File_Chmod wraps Go-native METHOD os.File.Chmod for disk failure tracking.
func File_Chmod(this *os.File, mode os.FileMode) error {
	err := this.Chmod(mode)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// File_Close wraps Go-native METHOD os.File.Close for disk failure tracking.
func File_Close(this *os.File) error {
	err := this.Close()
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// File_Read wraps Go-native METHOD os.File.Read for disk failure tracking.
func File_Read(this *os.File, b []byte) (n int, err error) {
	n, err = this.Read(b)
	if err != nil {
		countDiskFailures(err)
	}
	return n, err
}

// File_Stat wraps Go-native METHOD os.File.Stat for disk failure tracking.
func File_Stat(this *os.File) (os.FileInfo, error) {
	fileInfo, err := this.Stat()
	if err != nil {
		countDiskFailures(err)
	}
	return fileInfo, err
}

// File_Sync wraps Go-native METHOD os.File.Sync for disk failure tracking.
func File_Sync(this *os.File) error {
	err := this.Sync()
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// File_Write wraps Go-native METHOD os.File.Write for disk failure tracking.
func File_Write(this *os.File, b []byte) (n int, err error) {
	n, err = this.Write(b)
	if err != nil {
		countDiskFailures(err)
	}
	return n, err
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// io FUNCTION wrappers -- add more as needed
////////////////////////////////////////////////////////////////////////////////////////////////////

// Io_Copy wraps Go-native FUNCTION io.Copy for disk failure tracking.
func Io_Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	written, err = io.Copy(dst, src)
	if err != nil {
		countDiskFailures(err)
	}
	return written, err
}

// Io_ReadFull wraps Go-native FUNCTION io.ReadFull for disk failure tracking.
func Io_ReadFull(r io.Reader, buf []byte) (n int, err error) {
	n, err = io.ReadFull(r, buf)
	if err != nil {
		countDiskFailures(err)
	}
	return n, err
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ioutil FUNCTION wrappers -- add more as needed
////////////////////////////////////////////////////////////////////////////////////////////////////

// Ioutil_ReadAll wraps Go-native FUNCTION ioutil.ReadAll for disk failure tracking.
func Ioutil_ReadAll(r io.Reader) ([]byte, error) {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		countDiskFailures(err)
	}
	return bytes, err
}

// Ioutil_ReadDir wraps Go-native FUNCTION ioutil.ReadDir for disk failure tracking.
func Ioutil_ReadDir(dirname string) ([]fs.FileInfo, error) {
	fileInfo, err := ioutil.ReadDir(dirname)
	if err != nil {
		countDiskFailures(err)
	}
	return fileInfo, err
}

// Ioutil_ReadFile wraps Go-native FUNCTION ioutil.ReadFile for disk failure tracking.
func Ioutil_ReadFile(filename string) ([]byte, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		countDiskFailures(err)
	}
	return bytes, err
}

// Ioutil_TempDir wraps Go-native FUNCTION ioutil.TempDir for disk failure tracking.
func Ioutil_TempDir(dir, pattern string) (name string, err error) {
	name, err = ioutil.TempDir(dir, pattern)
	if err != nil {
		countDiskFailures(err)
	}
	return name, err
}

// Ioutil_TempFile wraps Go-native FUNCTION ioutil.TempFile for disk failure tracking.
func Ioutil_TempFile(dir, pattern string) (f *os.File, err error) {
	f, err = ioutil.TempFile(dir, pattern)
	if err != nil {
		countDiskFailures(err)
	}
	return f, err
}

// Ioutil_WriteFile wraps Go-native FUNCTION ioutil.WriteFile for disk failure tracking.
func Ioutil_WriteFile(filename string, data []byte, perm fs.FileMode) error {
	err := ioutil.WriteFile(filename, data, perm)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// os FUNCTION wrappers -- add more as needed
////////////////////////////////////////////////////////////////////////////////////////////////////

// Os_Chmod wraps Go-native FUNCTION os.Chmod for disk failure tracking.
func Os_Chmod(name string, mode os.FileMode) error {
	err := os.Chmod(name, mode)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_Create wraps Go-native FUNCTION os.Create for disk failure tracking.
func Os_Create(name string) (*os.File, error) {
	file, err := os.Create(name)
	if err != nil {
		countDiskFailures(err)
	}
	return file, err
}

// Os_Mkdir wraps Go-native FUNCTION os.MkDir for disk failure tracking.
func Os_Mkdir(name string, perm os.FileMode) error {
	err := os.Mkdir(name, perm)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_MkdirAll wraps Go-native FUNCTION os.MkDirAll for disk failure tracking.
func Os_MkdirAll(path string, perm os.FileMode) error {
	err := os.MkdirAll(path, perm)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_Open wraps Go-native FUNCTION os.Open for disk failure tracking.
func Os_Open(name string) (*os.File, error) {
	file, err := os.Open(name)
	if err != nil {
		countDiskFailures(err)
	}
	return file, err
}

// Os_OpenFile wraps Go-native FUNCTION os.OpenFile for disk failure tracking.
func Os_OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		countDiskFailures(err)
	}
	return file, err
}

// Os_Remove wraps Go-native FUNCTION os.Remove for disk failure tracking.
func Os_Remove(name string) error {
	err := os.Remove(name)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_RemoveAll wraps Go-native FUNCTION os.RemoveAll for disk failure tracking.
func Os_RemoveAll(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_Rename wraps Go-native FUNCTION os.Rename for disk failure tracking.
func Os_Rename(oldpath, newpath string) error {
	err := os.Rename(oldpath, newpath)
	if err != nil {
		countDiskFailures(err)
	}
	return err
}

// Os_Stat wraps Go-native FUNCTION os.Stat for disk failure tracking.
func Os_Stat(name string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(name)
	if err != nil {
		countDiskFailures(err)
	}
	return fileInfo, err
}
