package secondaryindex

import (
	"log"
	"os"
)

const (
	BLK_MARKER_SIZE     = 1
	BLK_MARKER_DBHEADER = 0xee
	BLK_MARKER_BAD      = 0xbd
	FDB_BLOCKSIZE       = 4096
)

type filemgr struct {
	pos       uint64
	blocksize uint32
	filepath  string
}

func NewFDBFilemgr(filepath string) (*filemgr, error) {
	fm := &filemgr{}
	fi, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}

	fm.pos = uint64(fi.Size())
	fm.filepath = filepath
	fm.blocksize = uint32(FDB_BLOCKSIZE)
	return fm, nil
}

func (file *filemgr) filemgrReadBlock(bid uint64) ([]byte, error) {
	f, err := os.OpenFile(file.filepath, os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	// Assuming no encryption
	b1 := make([]byte, file.blocksize)
	f.Seek(int64(uint64(file.blocksize)*bid), 0)
	_, err1 := f.Read(b1)
	if err1 != nil {
		return nil, err1
	}

	errClose := f.Close()
	if errClose != nil {
		return nil, errClose
	}

	return b1, nil
}

func (file *filemgr) writeDataToFile(buf []byte, offset uint64) error {
	f, err := os.OpenFile(file.filepath, os.O_RDWR, 0777)
	if err != nil {
		return err
	}

	_, err = f.WriteAt(buf, int64(offset))
	if err != nil {
		return err
	}

	errClose := f.Close()
	if errClose != nil {
		return errClose
	}

	return nil
}

func (file *filemgr) CorruptForestdbFile() error {
	var hdrBid, hdrBidLocal uint64
	hdrBid = file.pos/uint64(file.blocksize) - 1

	var err error
	var data []byte

	for hdrBidLocal = hdrBid; hdrBidLocal != 0; hdrBidLocal-- {
		data, err = file.filemgrReadBlock(hdrBidLocal)
		if err != nil {
			break
		}

		marker := data[uint64(file.blocksize)-BLK_MARKER_SIZE:]

		// Keep non-header blocks as they are
		if marker[0] != BLK_MARKER_DBHEADER {
			continue
		}

		offset := (hdrBidLocal * uint64(file.blocksize)) + uint64(file.blocksize) - BLK_MARKER_SIZE
		d := make([]byte, BLK_MARKER_SIZE)
		d[0] = BLK_MARKER_BAD
		err = file.writeDataToFile(d, offset)
		if err != nil {
			break
		}
		log.Println("Written data to offset ", offset)
		log.Println("FDBCorrupt: Corrupted header for block id", hdrBidLocal)
	}

	return err
}
