package memdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/couchbase/gocbcrypto"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/iowrap"
)

const DiskBlockSize = 4 * 1024 // 4K is ok for page cache writes

var (
	ErrNotEnoughSpace = errors.New("Not enough space in the buffer")
	forestdbConfig    *forestdb.Config
)

func init() {
	forestdbConfig = forestdb.DefaultConfig()
	forestdbConfig.SetSeqTreeOpt(forestdb.SEQTREE_NOT_USE)
	forestdbConfig.SetBufferCacheSize(1024 * 1024)

}

type FileWriter interface {
	Open() error
	WriteItem(*Item) error
	Checksum() uint32
	Close(sync bool) error
	FlushAndClose(sync bool) error
}

type FileReader interface {
	Open(path string) error
	ReadItem() (*Item, error)
	Checksum() uint32
	Close() error
}

func (m *MemDB) newFileWriter(t FileType, path string, keyId []byte, cipher string) (FileWriter, error) {
	var w FileWriter
	var err error
	if t == RawdbFile {
		w = &rawFileWriter{db: m, path: path}
		if len(keyId) > 0 {
			var ctx gocbcrypto.EncryptionContext
			if ctx, err = m.NewEncryptionContext(keyId, cipher); err == nil {
				w = &rawFileWriterEncrypted{w.(*rawFileWriter), ctx, nil, sync.Once{}}
			} else {
				w = nil
			}
		}
	} else if t == ForestdbFile {
		w = &forestdbFileWriter{db: m, path: path}
	} else {
		err = fmt.Errorf("unsupported file type")
	}

	return w, err
}

func (m *MemDB) newFileReader(t FileType, ver int, file string) (FileReader, error) {
	var r FileReader
	var err error
	if t == RawdbFile {
		r = &rawFileReader{db: m, version: ver}
		var ok bool
		if ok, err = gocbcrypto.IsFileEncrypted(file); ok {
			r = &rawFileReaderEncrypted{r.(*rawFileReader), m.GetEncryptionKeyById, nil}
		}
	} else if t == ForestdbFile {
		r = &forestdbFileReader{db: m}
	}

	return r, err
}

// rawFileWriter implements the FileWriter interface defined above.
type rawFileWriter struct {
	db       *MemDB
	fd       *os.File
	w        *bufio.Writer
	buf      []byte
	path     string
	checksum uint32
}

func (f *rawFileWriter) Open() error {
	var err error
	f.fd, err = iowrap.Os_OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err == nil {
		if f.buf == nil {
			f.buf = make([]byte, encodeBufSize)
		}

		f.w = bufio.NewWriterSize(f.fd, DiskBlockSize)
	}
	return err
}

// rawFileWriter.FlushAndClose flushes the buffer to the io.Writer (which does NOT
// force the bytes to disk), optionally syncs the file (which DOES force the bytes
// to disk), and closes the file. Returns the first error, if any.
func (f *rawFileWriter) FlushAndClose(sync bool) (reterr error) {

	if f.w != nil {
		reterr = f.w.Flush()
	}
	f.w = nil

	err := f.syncAndClose(sync)
	if reterr == nil {
		reterr = err
	}

	return reterr
}

func (f *rawFileWriter) syncAndClose(sync bool) (reterr error) {

	if f.fd != nil {
		if sync {
			err := iowrap.File_Sync(f.fd)
			if reterr == nil {
				reterr = err
			}
		}
		err := iowrap.File_Close(f.fd)
		if reterr == nil {
			reterr = err
		}
	}

	if reterr == nil {
		f.fd = nil
	}

	return reterr
}

func (f *rawFileWriter) WriteItem(itm *Item) error {
	checksum, err := f.db.EncodeItem(itm, f.buf, f.w)
	f.checksum = f.checksum ^ checksum
	return err
}

func (f *rawFileWriter) Checksum() uint32 {
	return f.checksum
}

// rawFileWriter.Close writes an empty Item terminator to the file, flushes the buffer to
// the io.Writer (which does NOT force the bytes to disk), optionally syncs the file (which
// DOES force the bytes to disk), and closes the file. Returns the first error, if any.
func (f *rawFileWriter) Close(sync bool) (reterr error) {

	// Open the file if needed
	if f.fd == nil {
		reterr = f.Open()
		if reterr != nil {
			return reterr
		}
	}

	// Write empty item terminator
	terminator := &Item{}
	err := f.WriteItem(terminator)
	if reterr == nil {
		reterr = err
	}

	err = f.FlushAndClose(sync)
	if reterr == nil {
		reterr = err
	}

	return reterr
}

// rawFileReader implements the FileReader interface defined above.
type rawFileReader struct {
	version  int
	db       *MemDB
	fd       *os.File
	r        *bufio.Reader
	buf      []byte
	path     string
	checksum uint32
}

func (f *rawFileReader) Open(path string) error {
	var err error
	f.fd, err = iowrap.Os_Open(path)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.r = bufio.NewReaderSize(f.fd, DiskBlockSize)
	}
	return err
}

func (f *rawFileReader) ReadItem() (*Item, error) {
	itm, checksum, err := f.db.DecodeItem(f.version, f.buf, f.r)
	if itm != nil { // StoreToDisk checksum excludes terminal nil item
		f.checksum = f.checksum ^ checksum
	}
	return itm, err
}

func (f *rawFileReader) Checksum() uint32 {
	return f.checksum
}

func (f *rawFileReader) Close() error {
	return iowrap.File_Close(f.fd)
}

// forestdbFileWriter implements the FileWriter interface defined above.
type forestdbFileWriter struct {
	db       *MemDB
	file     *forestdb.File
	store    *forestdb.KVStore
	buf      []byte
	wbuf     bytes.Buffer
	checksum uint32
	path     string
}

func (f *forestdbFileWriter) Open() error {
	var err error
	f.file, err = forestdb.Open(f.path, forestdbConfig)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.store, err = f.file.OpenKVStoreDefault(nil)
	}

	return err
}

func (f *forestdbFileWriter) WriteItem(itm *Item) error {
	f.wbuf.Reset()
	checksum, err := f.db.EncodeItem(itm, f.buf, &f.wbuf)
	if err == nil {
		f.checksum = checksum
		err = f.store.SetKV(f.wbuf.Bytes(), nil)
	}

	return err
}

func (f *forestdbFileWriter) Checksum() uint32 {
	return f.checksum
}

// forestdbFileWriter.FlushAndClose sync flag is ignored by delegate.
func (f *forestdbFileWriter) FlushAndClose(sync bool) error {
	return f.Close(sync)
}

// forestdbFileWriter.Close ignores the sync flag.
func (f *forestdbFileWriter) Close(sync bool) (reterr error) {
	if f.file != nil {
		if err := f.file.Commit(forestdb.COMMIT_NORMAL); err != nil {
			reterr = err
		}
	}

	if f.store != nil {
		if err := f.store.Close(); err != nil {
			reterr = err
		}
	}
	f.store = nil

	if f.file != nil {
		reterr = f.file.Close()
	}
	f.file = nil

	return reterr
}

// forestdbFileReader implements the FileReader interface defined above.
type forestdbFileReader struct {
	db       *MemDB
	file     *forestdb.File
	store    *forestdb.KVStore
	iter     *forestdb.Iterator
	buf      []byte
	checksum uint32
}

func (f *forestdbFileReader) Open(path string) error {
	var err error

	f.file, err = forestdb.Open(path, forestdbConfig)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.store, err = f.file.OpenKVStoreDefault(nil)
		if err == nil {
			f.iter, err = f.store.IteratorInit(nil, nil, forestdb.ITR_NONE)
		}
	}

	return err
}

func (f *forestdbFileReader) ReadItem() (*Item, error) {
	itm := &Item{}
	doc, err := f.iter.Get()
	if err == forestdb.FDB_RESULT_ITERATOR_FAIL {
		return nil, nil
	}

	f.iter.Next()
	if err == nil {
		rbuf := bytes.NewBuffer(doc.Key())
		itm, f.checksum, err = f.db.DecodeItem(0, f.buf, rbuf)
	}

	return itm, err
}

func (f *forestdbFileReader) Checksum() uint32 {
	return f.checksum
}

func (f *forestdbFileReader) Close() error {
	f.iter.Close()
	f.store.Close()
	return f.file.Close()
}

// a) it is a buffered writer similar to bufio. user must call Flush before Close
// b) It should be initialized per file because encryption context is per file.
// c) If the underlying file is opened and closed more than once, context still
// should not be reset else it can lead to corruption
// d) The encryption granularity is 32K.
type rawFileWriterEncrypted struct {
	*rawFileWriter
	ctx       gocbcrypto.EncryptionContext // per file context
	cw        *gocbcrypto.CryptFileWriter  // buffered writer, overrides rawFileWriter writer
	firstOpen sync.Once                    // file encryption header
}

// a) initializes encryption context from the file header
// b) The decryption granularity is 32K. It should be kept same as writer.
type rawFileReaderEncrypted struct {
	*rawFileReader
	getKey func([]byte) []byte         // use keyId from header to derive encryption key
	cr     *gocbcrypto.CryptFileReader // buffered reader
}

// a)encryption context should not be reset on file open
// b)open should never be retried on first Open error. This is
// because the encryption header is always written on first open and
// cannot be recreated if the open fails.
func (f *rawFileWriterEncrypted) Open() error {
	var err error
	defer func() {
		if err != nil && f.fd != nil {
			f.fd.Close()
			f.fd = nil
		}
	}()

	f.fd, err = iowrap.Os_OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err == nil {
		if f.buf == nil {
			f.buf = make([]byte, encodeBufSize)
		}

		f.cw, err = gocbcrypto.NewCryptFileWriter(f.fd, f.ctx, f.db.encryptChunkSize, false, iowrap.CountDiskFailures)
		// store file encryption metadata on first open by writer
		f.firstOpen.Do(func() {
			if err == nil {
				_, err = f.cw.WriteHeader()
			}
		})
	}

	return err
}

// encryption context should not be reset if item terminator
// is not yet written to the file.
func (f *rawFileWriterEncrypted) FlushAndClose(sync bool) (reterr error) {
	if f.cw != nil {
		reterr = f.cw.Flush()
		if reterr == nil {
			f.cw.Reset()
		}
	}
	f.cw = nil

	err := f.syncAndClose(sync)
	if reterr == nil {
		reterr = err
	}

	return reterr
}

func (f *rawFileWriterEncrypted) WriteItem(itm *Item) error {
	checksum, err := f.db.EncodeItem(itm, f.buf, f.cw)
	if err == nil {
		f.checksum = f.checksum ^ checksum
	}
	return err
}

func (f *rawFileWriterEncrypted) Close(sync bool) (reterr error) {
	// Open the file if needed
	if f.fd == nil {
		reterr = f.Open()
		if reterr != nil {
			return reterr
		}
	}

	// Write empty item terminator
	terminator := &Item{}
	err := f.WriteItem(terminator)
	if reterr == nil {
		reterr = err
	}

	err = f.FlushAndClose(sync)
	if reterr == nil {
		reterr = err
	}

	return reterr
}

///////////
// reader
///////////

// file should be opened and read till completion
func (f *rawFileReaderEncrypted) Open(path string) error {
	var err error
	defer func() {
		if err != nil && f.fd != nil {
			f.fd.Close()
			f.fd = nil
		}
	}()

	if f.fd, err = iowrap.Os_Open(path); err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.cr, err = gocbcrypto.NewCryptFileReaderWithLabel(f.fd, f.getKey, KDFLabelCtx, f.db.encryptChunkSize, false, iowrap.CountDiskFailures)
	}

	return err
}

func (f *rawFileReaderEncrypted) ReadItem() (*Item, error) {
	itm, checksum, err := f.db.DecodeItem(f.version, f.buf, f.cr)
	if itm != nil { // StoreToDisk checksum excludes terminal nil item
		f.checksum = f.checksum ^ checksum
	}
	return itm, err
}

func (f *rawFileReaderEncrypted) Close() (err error) {
	if f.cr != nil {
		f.cr.Reset()
	}
	f.cr = nil

	if f.fd != nil {
		err = iowrap.File_Close(f.fd)
	}

	if err == nil {
		f.fd = nil
	}
	return
}
