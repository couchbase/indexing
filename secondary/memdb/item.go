package memdb

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/indexing/secondary/logging"
	"hash/crc32"
	"io"
	"reflect"
	"unsafe"

	"github.com/couchbase/indexing/secondary/iowrap"
)

var itemHeaderSize = unsafe.Sizeof(Item{})

type Item struct {
	bornSn  uint32
	deadSn  uint32
	dataLen uint32
}

func (m *MemDB) newItem(data []byte, useMM bool) (itm *Item) {
	l := len(data)
	itm = m.allocItem(l, useMM)
	copy(itm.Bytes(), data)
	return itm
}

func (m *MemDB) freeItem(itm *Item) {
	if m.useMemoryMgmt {
		m.freeFun(unsafe.Pointer(itm))
	}
}

func (m *MemDB) allocItem(l int, useMM bool) (itm *Item) {
	blockSize := itemHeaderSize + uintptr(l)
	if useMM {
		itm = (*Item)(m.mallocFun(int(blockSize)))
		itm.deadSn = 0
		itm.bornSn = 0
	} else {
		block := make([]byte, blockSize)
		itm = (*Item)(unsafe.Pointer(&block[0]))
	}

	itm.dataLen = uint32(l)
	return
}

func (m *MemDB) EncodeItem(itm *Item, buf []byte, w io.Writer) (
	checksum uint32, err error) {
	l := 4
	if len(buf) < l {
		err = ErrNotEnoughSpace
		return
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(itm.dataLen))
	if _, err = w.Write(buf[0:4]); err != nil {
		return
	}
	checksum = crc32.ChecksumIEEE(buf[0:4])
	itmBytes := itm.Bytes()
	if _, err = w.Write(itmBytes); err != nil {
		return
	}
	checksum = checksum ^ crc32.ChecksumIEEE(itmBytes)

	return
}

func (m *MemDB) DecodeItem(ver int, buf []byte, r io.Reader) (*Item, uint32, error) {
	var l int
	var checksum uint32

	if ver == 0 {
		if _, err := iowrap.Io_ReadFull(r, buf[0:2]); err != nil {
			return nil, checksum, err
		}
		l = int(binary.BigEndian.Uint16(buf[0:2]))
		checksum = crc32.ChecksumIEEE(buf[0:2])
	} else {
		if _, err := iowrap.Io_ReadFull(r, buf[0:4]); err != nil {
			return nil, checksum, err
		}
		l = int(binary.BigEndian.Uint32(buf[0:4]))
		checksum = crc32.ChecksumIEEE(buf[0:4])
	}

	if l > 0 {
		itm := m.allocItem(l, m.useMemoryMgmt)
		data := itm.Bytes()
		_, err := iowrap.Io_ReadFull(r, data)
		if err == nil {
			checksum = checksum ^ crc32.ChecksumIEEE(data)
		}
		return itm, checksum, err
	}

	return nil, checksum, nil
}

func (itm *Item) String() string {
	return fmt.Sprintf("bornSn[%d] deadSn[%d] dataLen[%d] bytes[%s]", itm.bornSn, itm.deadSn, itm.dataLen, logging.TagStrUD(itm.Bytes()))
}

// Return copy of bytes
func (itm *Item) BytesCopy() []byte {
	bs := append([]byte{}, itm.Bytes()...)

	return bs
}

func (m *MemDB) CopyItem(src *Item) *Item {
	dst := m.newItem(src.Bytes(), m.useMemoryMgmt)
	dst.bornSn = src.bornSn
	dst.deadSn = src.deadSn
	return dst
}

// Return pointer to bytes
func (itm *Item) Bytes() (bs []byte) {
	l := itm.dataLen
	dataOffset := uintptr(unsafe.Pointer(itm)) + itemHeaderSize

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Data = dataOffset
	hdr.Len = int(l)
	hdr.Cap = hdr.Len
	return
}

func ItemSize(p unsafe.Pointer) int {
	itm := (*Item)(p)
	return int(itemHeaderSize + uintptr(itm.dataLen))
}
