// +build !amd64

package skiplist

import (
	"reflect"
	"sync/atomic"
	"unsafe"
)

//
// The default skiplist implementation :
// a) should support 64-bit platforms including aarch64 which has alignment restrictions
// (on arm64, 64-bit words accessed atomically should have 64-bit address alignment
// otherwise will result in alignment fault.)
// b) support working with golang memory (garbage collection safe)
// c) support working with user managed memory (e.g. jemalloc)
//
// Nitro's two-phase deletion approach requires us to atomically update both the
// next pointer as well as the state of the current node. Refer nitro paper for
// more details.
//
// For user-managed memory, this can be addressed by using tagged pointers.
// However this relies on the fact that the platform's virtual addresses do not use
// the entire 64 bits. To our knowledge, amd64 uses 48-bits VA addresses and for
// ARMV8.3 supporting large VA addressing mode (64K page size), it can be 52-bits.
//
// Below is the node layout we use for user managed memory
//
//  <NodeMM struct>
// +------------+-----------+-----------+
// | level - 8b | itm - 8b  |  Link - 8b|<NodeRefMM>
// +------------+-----------+-----------+-------------+--------------+
//                                      | tag ptr- 8b| tag ptr - 8b|
//                                      +-------------+--------------+
//
// For golang memory, this is addressed by using an indirection pointer.
// A NodeRef pointer is stored in skiplist levels which point to an object which
// contains both the state & the next pointer.
//
// Below is the node layout we use for golang memory
//
//  <Node struct>
// +------------+------------+-----------+-------------+
// | level - 8b | next - 8b  | itm - 8b  |  Link - 8b  |
// +------------+------------+-----------+-------------+
//              | ----- |------------------+----------------+
//			| NodeRef ptr - 8b | NodeRefptr - 8b|
//                      |------------------+----------------+
//
// Note: Although golang indirection approach can work with user managed memory,
// but it comes with an overhead of constant memory allocation/deallocation in
// case of conflicts and also SMR will not be straightforward. Reclaim in SMR
// becomes easy if we allocate node memory as a single blob (NodeMM).
//
// Based on type of memory management used for skiplist, we cache the
// type information in the MSB of level field. Currently MaxLevel is 32.
//

// 52-bit Large VA address capability is supported from ARMv8.2 onwards (64KB page size)
const deletedFlag = uint64(1) << 52
const deletedFlagMask = ^deletedFlag

const mmFlag = int(1) << 62
const mmFlagMask = (^mmFlag)

var nodeHdrSizeMM = unsafe.Sizeof(NodeMM{})
var nodeRefSizeMM = unsafe.Sizeof(NodeRefMM{})

// Node represents skiplist entry
// This should reside in a single cache line (L1 cache 64bytes)
type Node struct {
	level  int // we use the 63rd bit to store node type (so max level can be 4611686018427387904)
	itm    unsafe.Pointer
	GClink *Node
	next   unsafe.Pointer // Points to [level+1]unsafe.Pointer
}

// NodeRef is a wrapper for node pointer
type NodeRef struct {
	deleted bool
	ptr     *Node
}

// NodeMM represents skiplist entry from user managed memory.
// We skips the next pointer in Node struct to save bytes
type NodeMM struct {
	level  int
	itm    unsafe.Pointer
	GClink *Node
}

// NodeRefMM is a wrapper for Node(MM) pointer tagged with deletedFlag
type NodeRefMM struct {
	tagptr uint64
}

// for user managed memory
func (n *Node) setMM() {
	n.level |= mmFlag
}

// this is inlined by go as seen from profile
func (n *Node) usesMM() bool {
	return (n.level & mmFlag) != 0
}

// get a slice of NodeRef's containing golang pointers
func (n *Node) nextArray() (s []unsafe.Pointer) {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = uintptr(n.next)
	hdr.Len = n.Level() + 1
	hdr.Cap = hdr.Len
	return
}

// Level returns the level of a node in the skiplist
func (n Node) Level() int {
	return n.level & mmFlagMask
}

func (n Node) Size() int {
	if n.usesMM() {
		return int(nodeHdrSizeMM + uintptr(n.Level()+1)*nodeRefSizeMM)
	} else {
		return int(unsafe.Sizeof(n) +
			uintptr(n.Level()+1)*(unsafe.Sizeof(unsafe.Pointer(nil))+
				unsafe.Sizeof(NodeRef{})))
	}
}

func (n *Node) Item() unsafe.Pointer {
	return n.itm
}

func (n *Node) SetLink(l *Node) {
	n.GClink = l
}

func (n *Node) GetLink() *Node {
	return n.GClink
}

func allocNode(itm unsafe.Pointer, level int, fn MallocFn) *Node {
	var n *Node
	// we reserve level's 63rd bit (1-based) to cache node type
	if level < 0 || level >= mmFlag {
		return nil
	}
	if fn == nil {
		next := make([]unsafe.Pointer, level+1)
		n = &Node{
			level: level,
			next:  unsafe.Pointer(&next[0]),
		}
	} else {
		// NodeMM is casted as Node (NodeMM is not undersized)
		n = (*Node)(fn(int(nodeHdrSizeMM + uintptr(level+1)*nodeRefSizeMM)))
		if n == nil {
			return nil
		}
		n.level = level
		n.GClink = nil
		n.setMM() // malloced memory
	}

	n.itm = itm
	return n
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	if n.usesMM() {
		nodeRefAddr := (uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) +
			nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)))
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		tag := uint64(uintptr(unsafe.Pointer(ptr)))
		if deleted {
			tag |= deletedFlag
		}
		atomic.StoreUint64(wordAddr, tag)
	} else {
		next := n.nextArray()
		next[level] = unsafe.Pointer(&NodeRef{ptr: ptr, deleted: deleted})
	}
}

func (n *Node) getNext(level int) (*Node, bool) {
	if n.usesMM() {
		nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		v := atomic.LoadUint64(wordAddr)
		ptr := (*Node)(unsafe.Pointer(uintptr(v) & uintptr(deletedFlagMask)))
		if ptr != nil {
			return ptr, (v&deletedFlag != uint64(0))
		}
	} else {
		next := n.nextArray()
		ref := (*NodeRef)(atomic.LoadPointer(&next[level]))
		if ref != nil {
			return ref.ptr, ref.deleted
		}
	}

	return nil, false
}

func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	var swapped bool
	if n.usesMM() {
		nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		prevVal := uint64(uintptr(unsafe.Pointer(prevPtr)))
		newVal := uint64(uintptr(unsafe.Pointer(newPtr)))

		if prevIsdeleted {
			prevVal |= deletedFlag
		}

		if newIsdeleted {
			newVal |= deletedFlag
		}
		swapped = atomic.CompareAndSwapUint64(wordAddr, prevVal, newVal)
	} else {
		next := n.nextArray()
		addr := &next[level]
		ref := (*NodeRef)(atomic.LoadPointer(addr))
		if (ref == nil) || (ref.ptr == prevPtr && ref.deleted == prevIsdeleted) {
			swapped = atomic.CompareAndSwapPointer(addr, unsafe.Pointer(ref),
				unsafe.Pointer(&NodeRef{ptr: newPtr, deleted: newIsdeleted}))
		}
	}
	return swapped
}

// debugMarkFree was undefined for non intel architectures so added the placeholder
func debugMarkFree(*Node) {
	return
}
