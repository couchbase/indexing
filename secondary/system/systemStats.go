package system

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
//#include <sigar_control_group.h>
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

// SigarCpuT type Go-wraps the sigar C library sigar_cpu_t type. CPU in use should sum Sys + User +
// Nice + Irq + SoftIrq. (This is different from the sigar_cpu_perc_calculate function's
// perc.combined calculation, whose semantics are unclear.)
type SigarCpuT struct {
	Sys     uint64 // CPU executing system thread
	User    uint64 // CPU executing user thread at normal priority [-20, 0]
	Nice    uint64 // CPU executing user thread at low priority [1, 20]
	Irq     uint64 // CPU executing "top half" (uninterruptible) part of a HW interrupt handler
	SoftIrq uint64 // CPU executing "bottom half" (long, slow) part of a HW interrupt handler
	Wait    uint64 // CPU not executing anything but an IO is outstanding (a type of idle time)
	Idle    uint64 // CPU not executing anything and no IO is outstanding
	Stolen  uint64 // CPU time given to other virtual machines in a VM or cloud environment

	Total uint64 // total elapsed time
}

// SigarCpuGet Go-wraps the sigar C library sigar_cpu_get function.
func (h *SystemStats) SigarCpuGet() (*SigarCpuT, error) {
	var cpu C.sigar_cpu_t
	if err := C.sigar_cpu_get(h.handle, &cpu); err != C.SIGAR_OK {
		return nil, errors.New(fmt.Sprintf("sigar_cpu_get failed.  Err=%v", C.sigar_strerror(h.handle, err)))
	}
	return &SigarCpuT{
		Sys:     uint64(cpu.sys),
		User:    uint64(cpu.user),
		Nice:    uint64(cpu.nice),
		Irq:     uint64(cpu.irq),
		SoftIrq: uint64(cpu.soft_irq),
		Wait:    uint64(cpu.wait),
		Idle:    uint64(cpu.idle),
		Stolen:  uint64(cpu.stolen),

		Total: uint64(cpu.total),
	}, nil
}

type SigarControlGroupInfo struct {
	Supported uint8 // "1" if cgroup info is supprted, "0" otherwise
	Version   uint8 // "1" for cgroup v1, "2" for cgroup v2

	// The number of CPUs available in the cgroup (in % where 100% represents 1 full core)
	// Derived from (cpu.cfs_quota_us/cpu.cfs_period_us) or COUCHBASE_CPU_COUNT env variable
	NumCpuPrc uint16

	// Maximum memory available in the group. Derived from memory.max
	MemoryMax uint64

	// Current memory usage by this cgroup. Derived from memory.usage_in_bytes
	MemoryCurrent uint64

	// UsageUsec gives the total microseconds of CPU used from sigar start across all available
	// cores, so this can increase at a rate of N times real time if there are N cores in use
	UsageUsec uint64
}

func (h *SystemStats) GetControlGroupInfo() *SigarControlGroupInfo {
	var info C.sigar_control_group_info_t
	C.sigar_get_control_group_info(&info)

	return &SigarControlGroupInfo{
		Supported:     uint8(info.supported),
		Version:       uint8(info.version),
		NumCpuPrc:     uint16(info.num_cpu_prc),
		MemoryMax:     uint64(info.memory_max),
		MemoryCurrent: uint64(info.memory_current),
		UsageUsec:     uint64(info.usage_usec),
	}
}
