// Package sstable implements immutable Sorted String Tables.
// Format: [data block][index block][footer(16 bytes)]
// Each KV record: key_len(4) | key | value_len(4) | value | version(8) | deleted(1)
package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/makhskham/oncloudkv/internal/storage/bloom"
)

const (
	magic        uint64 = 0x4F434B56_53535442 // "OCKVSSTB"
	footerSize          = 24                  // indexOffset(8)+indexLen(4)+count(4)+magic(8)
	defaultItems        = 1000
)

// Entry is a single record in an SSTable.
type Entry struct {
	Key     string
	Value   []byte
	Version int64
	Deleted bool
}

// IndexEntry points to an entry's byte offset in the data block.
type IndexEntry struct {
	Key    string
	Offset int64
}

// Writer writes a sorted sequence of entries into an SSTable file.
type Writer struct {
	f      *os.File
	bw     *bufio.Writer
	index  []IndexEntry
	offset int64
	count  int
}

// NewWriter opens path for writing. Entries must be written in sorted key order.
func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, bw: bufio.NewWriterSize(f, 1<<20)}, nil
}

// Write appends one entry. Keys must arrive in ascending order.
func (w *Writer) Write(e Entry) error {
	w.index = append(w.index, IndexEntry{Key: e.Key, Offset: w.offset})

	keyBytes := []byte(e.Key)
	rec := make([]byte, 4+len(keyBytes)+4+len(e.Value)+8+1)
	pos := 0
	binary.BigEndian.PutUint32(rec[pos:], uint32(len(keyBytes)))
	pos += 4
	copy(rec[pos:], keyBytes)
	pos += len(keyBytes)
	binary.BigEndian.PutUint32(rec[pos:], uint32(len(e.Value)))
	pos += 4
	copy(rec[pos:], e.Value)
	pos += len(e.Value)
	binary.BigEndian.PutUint64(rec[pos:], uint64(e.Version))
	pos += 8
	if e.Deleted {
		rec[pos] = 1
	}

	n, err := w.bw.Write(rec)
	w.offset += int64(n)
	w.count++
	return err
}

// Close finalises the SSTable: writes the index block and footer, then closes.
func (w *Writer) Close() error {
	indexStart := w.offset

	// write index block
	for _, ie := range w.index {
		kb := []byte(ie.Key)
		hdr := make([]byte, 4+len(kb)+8)
		binary.BigEndian.PutUint32(hdr, uint32(len(kb)))
		copy(hdr[4:], kb)
		binary.BigEndian.PutUint64(hdr[4+len(kb):], uint64(ie.Offset))
		if _, err := w.bw.Write(hdr); err != nil {
			return err
		}
	}
	indexLen := int64(0)
	for _, ie := range w.index {
		indexLen += int64(4 + len(ie.Key) + 8)
	}

	// footer
	footer := make([]byte, footerSize)
	binary.BigEndian.PutUint64(footer[0:], uint64(indexStart))
	binary.BigEndian.PutUint32(footer[8:], uint32(indexLen))
	binary.BigEndian.PutUint32(footer[12:], uint32(w.count))
	binary.BigEndian.PutUint64(footer[16:], magic)
	if _, err := w.bw.Write(footer); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// Reader reads an immutable SSTable file.
type Reader struct {
	f      *os.File
	index  []IndexEntry
	filter *bloom.Filter
	count  int
}

// OpenReader opens an existing SSTable for reads.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &Reader{f: f}
	if err := r.loadIndex(); err != nil {
		f.Close()
		return nil, err
	}
	r.buildBloom()
	return r, nil
}

func (r *Reader) loadIndex() error {
	// read footer
	if _, err := r.f.Seek(-footerSize, io.SeekEnd); err != nil {
		return err
	}
	footer := make([]byte, footerSize)
	if _, err := io.ReadFull(r.f, footer); err != nil {
		return err
	}
	if binary.BigEndian.Uint64(footer[16:]) != magic {
		return errors.New("sstable: invalid magic - file corrupt or not an SSTable")
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer[0:]))
	indexLen := int(binary.BigEndian.Uint32(footer[8:]))
	r.count = int(binary.BigEndian.Uint32(footer[12:]))

	if _, err := r.f.Seek(indexOffset, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, indexLen)
	if _, err := io.ReadFull(r.f, buf); err != nil {
		return err
	}

	pos := 0
	for pos < len(buf) {
		klen := int(binary.BigEndian.Uint32(buf[pos:]))
		pos += 4
		key := string(buf[pos : pos+klen])
		pos += klen
		offset := int64(binary.BigEndian.Uint64(buf[pos:]))
		pos += 8
		r.index = append(r.index, IndexEntry{Key: key, Offset: offset})
	}
	return nil
}

func (r *Reader) buildBloom() {
	r.filter = bloom.New(uint(r.count)+1, 0.01)
	for _, ie := range r.index {
		r.filter.Add([]byte(ie.Key))
	}
}

// Get looks up key. Returns (entry, true) on hit, (_, false) on miss.
func (r *Reader) Get(key string) (Entry, bool, error) {
	if !r.filter.Contains([]byte(key)) {
		return Entry{}, false, nil
	}
	// binary search in index
	lo, hi := 0, len(r.index)-1
	idx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if r.index[mid].Key == key {
			idx = mid
			break
		} else if r.index[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if idx == -1 {
		return Entry{}, false, nil
	}
	return r.readAt(r.index[idx].Offset)
}

func (r *Reader) readAt(offset int64) (Entry, bool, error) {
	if _, err := r.f.Seek(offset, io.SeekStart); err != nil {
		return Entry{}, false, err
	}
	return readEntry(r.f)
}

func readEntry(rd io.Reader) (Entry, bool, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return Entry{}, false, err
	}
	keyBytes := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(rd, keyBytes); err != nil {
		return Entry{}, false, err
	}
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return Entry{}, false, err
	}
	valBytes := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(rd, valBytes); err != nil {
		return Entry{}, false, err
	}
	var tail [9]byte
	if _, err := io.ReadFull(rd, tail[:]); err != nil {
		return Entry{}, false, err
	}
	return Entry{
		Key:     string(keyBytes),
		Value:   valBytes,
		Version: int64(binary.BigEndian.Uint64(tail[:8])),
		Deleted: tail[8] == 1,
	}, true, nil
}

// Scan iterates all entries with key >= start in sorted order.
func (r *Reader) Scan(start string, limit int) ([]Entry, error) {
	idx := 0
	for idx < len(r.index) && r.index[idx].Key < start {
		idx++
	}
	if idx >= len(r.index) {
		return nil, nil
	}
	if _, err := r.f.Seek(r.index[idx].Offset, io.SeekStart); err != nil {
		return nil, err
	}
	var results []Entry
	for {
		if limit > 0 && len(results) >= limit {
			break
		}
		e, ok, err := readEntry(r.f)
		if err != nil || !ok {
			break
		}
		results = append(results, e)
	}
	return results, nil
}

// Count returns the number of entries.
func (r *Reader) Count() int { return r.count }

// Close releases the file descriptor.
func (r *Reader) Close() error { return r.f.Close() }
