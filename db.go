package logdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cespare/xxhash"
)

// TODO implement batch inserts.
// TODO build index in background.
// TODO implement compaction - remove duplicate keys and tombstones.

var ErrNotExists = errors.New("key does not exists")

type Database struct {
	f *os.File // Used only for insert.

	hashFunc func(b []byte) uint64

	indexMu sync.RWMutex
	index   map[uint64]header
}

func Open(path string) (*Database, error) {
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return nil, fmt.Errorf("create dir %s: %q", filepath.Dir(path), err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("create file %s: %q", path, err)
	}

	db := &Database{
		f:        f,
		hashFunc: xxhash.Sum64,
		index:    make(map[uint64]header),
	}

	if err := db.buildIndex(); err != nil {
		return nil, fmt.Errorf("build index: %w", err)
	}

	return db, nil
}

func (d *Database) Set(key, value []byte) error {
	return d.save(key, value, false, true)
}

func (d *Database) BatchSet(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values have different length")
	}

	var err error

	for i := 0; i < len(keys); i++ {
		err = d.save(keys[i], values[i], false, false)
	}

	if err != nil {
		err = d.f.Sync()
	}

	return err
}

func (d *Database) Get(key []byte) ([]byte, error) {
	e, err := d.get(key)
	if err != nil {
		return nil, err
	}

	return e.value, nil
}

func (d *Database) Delete(key []byte) error {
	e, err := d.get(key)
	if err != nil {
		return err
	}

	return d.save(e.key, nil, true, true)
}

func (d *Database) Close() error {
	err := d.f.Sync()

	if err == nil {
		err = d.f.Close()
	}

	return err
}

// get retrieves the entry using index.
func (d *Database) get(key []byte) (*entry, error) {
	hashKey := d.hashFunc(key)

	d.indexMu.RLock()
	h, exists := d.index[hashKey]
	d.indexMu.RUnlock()

	if !exists {
		return nil, ErrNotExists
	}

	if _, err := d.f.Seek(h.offset, io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, h.klen+h.vlen)

	_, err := io.ReadFull(d.f, buf[:])
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("read entry, unexpected EOF: %w", err)
		}
		return nil, fmt.Errorf("read entry: %w", err)
	}

	e := &entry{}
	e.key = buf[:h.klen]
	e.value = buf[h.klen:]

	return e, nil
}

// find retrieves the last entry of the key, without using index.
func (d *Database) find(key []byte) (*entry, error) {
	f, err := os.OpenFile(d.f.Name(), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	var found *entry
	var h header

	for {
		err := h.decodeFrom(r)
		if err != nil {
			if err == io.EOF {
				if found == nil {
					return nil, ErrNotExists
				}
				return found, nil
			} else if err != nil {
				return nil, err
			}
		}

		e, _, err := decodeEntry(r, h, true)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("read entry, unexpected EOF: %w", err)
			}
			return nil, fmt.Errorf("read entry: %w", err)
		}

		if bytes.Equal(e.key, key) {
			found = e
		}
	}
}

// save writes the entry into disk.
//
// value can be nil.
// If isDelete is true, the value will not be saved.
func (d *Database) save(key []byte, value []byte, isDelete bool, fsync bool) error {
	h := header{
		klen: uint32(len(key)),
	}

	bufSize := MaxHeaderSize + len(key)

	if isDelete {
		h.meta |= 1
		h.vlen = 0
	} else {
		h.vlen = uint32(len(value))

		bufSize += len(value)
	}

	buf := make([]byte, bufSize)

	// Encode the header.
	hsize := h.encode(buf[:])
	bufN := hsize

	// Write the key.
	copy(buf[bufN:], key)
	bufN += len(key)

	// Write the value.
	// Don't save the entry's value if the entry is deleted.
	if !isDelete {
		copy(buf[bufN:], value)
		bufN += len(value)
	}

	// Make sure the pointer is at the end.
	offset, err := d.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	n, err := d.f.Write(buf[:bufN])
	if err != nil {
		return err
	}
	if n != bufN {
		return fmt.Errorf("want n %d, got %d: %w", len(buf),
			n, io.ErrShortWrite)
	}

	if fsync {
		err := d.f.Sync()
		if err != nil {
			return err
		}
	}

	keyHash := d.hashFunc(key)

	h.offset = offset + int64(hsize)

	d.indexMu.Lock()
	if isDelete {
		delete(d.index, keyHash)
	} else {
		d.index[keyHash] = h
	}
	d.indexMu.Unlock()

	return nil
}

func (d *Database) buildIndex() error {
	f, err := os.OpenFile(d.f.Name(), os.O_RDONLY, 0600)
	if err != nil {
		return fmt.Errorf("open file %s: %q", d.f.Name(), err)
	}
	defer f.Close()

	index := make(map[uint64]header)

	r := bufio.NewReader(f)
	rc := &readerCount{
		reader: r,
	}

	var h header

	for {
		err := h.decodeFrom(rc)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Record the offset for entry.
		offset := int64(rc.bytesRead)

		e, _, err := decodeEntry(rc, h, false)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF, expected entry: %w", err)
			}
			return err
		}

		h.offset = offset
		index[d.hashFunc(e.key)] = h
	}

	d.indexMu.Lock()
	d.index = index
	d.indexMu.Unlock()

	return nil
}

type readerCount struct {
	reader    *bufio.Reader
	bytesRead int
}

func (r *readerCount) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.bytesRead += n
	return
}

func (r *readerCount) ReadByte() (b byte, err error) {
	b, err = r.reader.ReadByte()
	if err == nil {
		r.bytesRead++
	}
	return
}

func (r *readerCount) Discard(i int) (n int, err error) {
	n, err = r.reader.Discard(i)
	r.bytesRead += n
	return
}
