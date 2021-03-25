package system

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
import "C"

import (
	"errors"
	"fmt"
)

type SystemStats struct {
	handle *C.sigar_t
	pid    C.sigar_pid_t
}

//
// Open a new handle
//
func NewSystemStats() (*SystemStats, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, errors.New(fmt.Sprintf("Fail to open sigar.  Error code = %v", err))
	}

	h := &SystemStats{}
	h.handle = handle
	h.pid = C.sigar_pid_get(handle)

	return h, nil
}

//
// Close handle
//
func (h *SystemStats) Close() {
	C.sigar_close(h.handle)
}

//
// Get CPU percentage
//
func (h *SystemStats) ProcessCpuPercent() (C.sigar_pid_t, float64, error) {

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(h.handle, h.pid, &cpu); err != C.SIGAR_OK {
		return C.sigar_pid_t(0), float64(0), errors.New(fmt.Sprintf("Fail to get CPU.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return h.pid, float64(cpu.percent) * 100, nil
}

//
// Get RSS
//
func (h *SystemStats) ProcessRSS() (C.sigar_pid_t, uint64, error) {

	var mem C.sigar_proc_mem_t
	if err := C.sigar_proc_mem_get(h.handle, h.pid, &mem); err != C.SIGAR_OK {
		return C.sigar_pid_t(0), uint64(0), errors.New(fmt.Sprintf("Fail to get RSS.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return h.pid, uint64(mem.resident), nil
}

//
// Get Free memory
//
func (h *SystemStats) FreeMem() (uint64, error) {

	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(h.handle, &mem); err != C.SIGAR_OK {
		return uint64(0), errors.New(fmt.Sprintf("Fail to get free memory.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return uint64(mem.free), nil
}

//
// Get Actual Free memory
// FreeMem() does not account for kernel buffer cache
// Hence, using sigar_mem_t::actual_free which accounts for kernel
// buffer cache when computing free memory
//
func (h *SystemStats) ActualFreeMem() (uint64, error) {

	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(h.handle, &mem); err != C.SIGAR_OK {
		return uint64(0), errors.New(fmt.Sprintf("Fail to get free memory.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return uint64(mem.actual_free), nil
}

//
// Get Total memory
//
func (h *SystemStats) TotalMem() (uint64, error) {

	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(h.handle, &mem); err != C.SIGAR_OK {
		return uint64(0), errors.New(fmt.Sprintf("Fail to get total memory.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return uint64(mem.total), nil
}
