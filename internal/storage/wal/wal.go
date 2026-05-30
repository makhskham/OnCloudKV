// Package wal implements a write-ahead log with group-commit support.
// Records are checksummed (CRC32) and synced before acknowledgement.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	recordHeaderSize = 8 // 4-byte length + 4-byte CRC32
	syncInterval     = 2 * time.Millisecond
	defaultBufSize   = 1 << 20 // 1 MiB
)

var ErrChecksumMismatch = errors.New("wal: checksum mismatch - log may be corrupt")

// WAL is an append-only, crash-safe write-ahead log.
// Group commit coalesces concurrent writes into a single fsync.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	bw     *bufio.Writer
	path   string
	closed bool

	// group commit channels
	pending []commitRequest
	syncCh  chan struct{}
	doneCh  chan struct{}
}

type commitRequest struct {
	data   []byte
	result chan error
}

// Open creates or opens the WAL at path.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		file:   f,
		bw:     bufio.NewWriterSize(f, defaultBufSize),
		path:   path,
		syncCh: make(chan struct{}, 1),
		doneCh: make(chan struct{}),
	}
	go w.groupCommitLoop()
	return w, nil
}

// Append durably writes a record to the WAL.
// It blocks until the record is fsynced to disk.
func (w *WAL) Append(data []byte) error {
	req := commitRequest{
		data:   data,
		result: make(chan error, 1),
	}
	w.mu.Lock()
	w.pending = append(w.pending, req)
	select {
	case w.syncCh <- struct{}{}:
	default:
	}
	w.mu.Unlock()
	return <-req.result
}

// groupCommitLoop drains pending writes in batches, fsyncing once per batch.
func (w *WAL) groupCommitLoop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case _, ok := <-w.syncCh:
			if !ok {
				return
			}
		}

		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		batch := w.pending
		w.pending = nil
		w.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		var writeErr error
		for _, req := range batch {
			if writeErr == nil {
				writeErr = w.writeRecord(req.data)
			}
		}

		var syncErr error
		if writeErr == nil {
			if flushErr := w.bw.Flush(); flushErr != nil {
				syncErr = flushErr
			} else {
				syncErr = w.file.Sync()
			}
		}

		for _, req := range batch {
			if writeErr != nil {
				req.result <- writeErr
			} else {
				req.result <- syncErr
			}
		}
	}
}

func (w *WAL) writeRecord(data []byte) error {
	checksum := crc32.ChecksumIEEE(data)
	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], checksum)
	if _, err := w.bw.Write(header); err != nil {
		return err
	}
	_, err := w.bw.Write(data)
	return err
}

// ReadAll replays all records from the WAL for crash recovery.
func ReadAll(dir string) ([][]byte, error) {
	path := filepath.Join(dir, "wal.log")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records [][]byte
	for {
		header := make([]byte, recordHeaderSize)
		if _, err := io.ReadFull(f, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(header[0:4])
		expected := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			break
		}
		if crc32.ChecksumIEEE(data) != expected {
			return records, ErrChecksumMismatch
		}
		records = append(records, data)
	}
	return records, nil
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	w.closed = true
	close(w.syncCh)
	w.mu.Unlock()
	<-w.doneCh
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
