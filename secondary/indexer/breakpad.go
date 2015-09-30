package indexer

//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//
// Golang interface to Google Breakpad crash-catching library, via our
// breakpad_wrapper library (which exposes a C API to Breakpad).
//

// #cgo LDFLAGS: -lbreakpad_wrapper
// #include <breakpad_wrapper/breakpad_wrapper.h>
// #include <stdlib.h>
import "C"
import "unsafe"

// Initialize sets up Breakpad, allowing to subsequenly generate
// Minidump files of the current process state.
// Typically invoked as part of program initialization, to reserve
// memory for Breakpad's operation. Must be called before attempting to
// write a minidump.
// @param minidump_dir Path to an existing, writable directory which
//        minidumps will be written to.
func BreakpadInitialize(minidump_dir string) {
	path := C.CString(minidump_dir)
	C.breakpad_initialize(path)
	C.free(unsafe.Pointer(path))
}

func BreakpadWriteMinidump() {
     C.breakpad_write_minidump()
}

func BreakpadGetWriteMinidumpAsUintptr() uintptr {
    return uintptr(C.breakpad_get_write_minidump_addr())
}
