// Package storage is the LSM-tree storage engine.
// Write path:  WAL → memtable → (flush) → SSTable
// Read path:   memtable → SSTables (newest first, bloom-filtered)
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/storage/compaction"
	"github.com/makhskham/oncloudkv/internal/storage/skiplist"
	"github.com/makhskham/oncloudkv/internal/storage/sstable"
	"github.com/makhskham/oncloudkv/internal/storage/wal"
)

const defaultMemtableSizeMB = 64

// Engine is the LSM-tree storage engine exposed to the Raft FSM.
type Engine struct {
	mu       sync.RWMutex
	memtable *skiplist.Skiplist
	immutable *skiplist.Skiplist // being flushed - read-only
	wal      *wal.WAL
	compact  *compaction.Compactor
	dir      string
	maxMem   int64 // memtable size threshold in bytes
	flushCh  chan struct{}
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// Entry is the public view of a stored record.
type Entry struct {
	Key     string
	Value   []byte
	Version int64
	Deleted bool
}

type walRecord struct {
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Version int64  `json:"version"`
}

// Open initialises the engine at dir, recovering from WAL if present.
func Open(dir string, memtableSizeMB int64) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if memtableSizeMB <= 0 {
		memtableSizeMB = defaultMemtableSizeMB
	}

	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}

	compact := compaction.New(filepath.Join(dir, "sst"), 30*time.Second)

	e := &Engine{
		memtable: skiplist.New(),
		wal:      w,
		compact:  compact,
		dir:      dir,
		maxMem:   memtableSizeMB << 20,
		flushCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	if err := e.recover(); err != nil {
		return nil, err
	}

	compact.Start()
	go e.flushLoop()
	return e, nil
}

// recover replays WAL records into the memtable.
func (e *Engine) recover() error {
	records, err := wal.ReadAll(filepath.Join(e.dir, "wal"))
	if err != nil {
		log.Warn().Err(err).Msg("WAL recovery partial - truncated or corrupt tail ignored")
	}
	for _, raw := range records {
		var rec walRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			continue
		}
		switch rec.Op {
		case "put":
			e.memtable.Put(rec.Key, rec.Value, rec.Version)
		case "delete":
			e.memtable.Delete(rec.Key, rec.Version)
		}
	}
	log.Info().Int("records", len(records)).Msg("WAL recovery complete")
	return nil
}

// Put durably writes key=value at version.
func (e *Engine) Put(key string, value []byte, version int64) error {
	rec, _ := json.Marshal(walRecord{Op: "put", Key: key, Value: value, Version: version})
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	e.mu.Lock()
	e.memtable.Put(key, value, version)
	needFlush := e.memtable.MemSize() >= e.maxMem
	e.mu.Unlock()

	if needFlush {
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// Delete marks key as deleted at version.
func (e *Engine) Delete(key string, version int64) error {
	rec, _ := json.Marshal(walRecord{Op: "delete", Key: key, Version: version})
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	e.mu.Lock()
	e.memtable.Delete(key, version)
	e.mu.Unlock()
	return nil
}

// Get retrieves the latest value for key.
func (e *Engine) Get(key string) (Entry, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. active memtable
	if entry, ok := e.memtable.Get(key); ok {
		return toEntry(entry), !entry.Deleted
	}
	// 2. immutable memtable (being flushed)
	if e.immutable != nil {
		if entry, ok := e.immutable.Get(key); ok {
			return toEntry(entry), !entry.Deleted
		}
	}
	// 3. SSTables (newest first)
	for _, path := range e.compact.Tables() {
		r, err := sstable.OpenReader(path)
		if err != nil {
			continue
		}
		entry, found, err := r.Get(key)
		r.Close()
		if err != nil || !found {
			continue
		}
		return Entry{
			Key:     entry.Key,
			Value:   entry.Value,
			Version: entry.Version,
			Deleted: entry.Deleted,
		}, !entry.Deleted
	}
	return Entry{}, false
}

// Scan returns entries with key >= prefix, up to limit (0 = unlimited).
func (e *Engine) Scan(prefix string, limit int) []Entry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	seen := make(map[string]Entry)

	addEntries := func(entries []skiplist.Entry) {
		for _, se := range entries {
			if _, ok := seen[se.Key]; !ok {
				seen[se.Key] = toEntry(se)
			}
		}
	}

	addEntries(e.memtable.Scan(prefix, "", limit))
	if e.immutable != nil {
		addEntries(e.immutable.Scan(prefix, "", limit))
	}

	for _, path := range e.compact.Tables() {
		r, err := sstable.OpenReader(path)
		if err != nil {
			continue
		}
		entries, err := r.Scan(prefix, limit)
		r.Close()
		if err != nil {
			continue
		}
		for _, se := range entries {
			if _, ok := seen[se.Key]; !ok {
				seen[se.Key] = Entry{
					Key:     se.Key,
					Value:   se.Value,
					Version: se.Version,
					Deleted: se.Deleted,
				}
			}
		}
	}

	var results []Entry
	for _, e := range seen {
		if !e.Deleted {
			results = append(results, e)
		}
	}
	return results
}

// Snapshot iterates all live entries for Raft FSM snapshotting.
func (e *Engine) Snapshot() []Entry {
	all := e.Scan("", 0)
	return all
}

// Restore replaces all engine data with the provided entries (used by Raft restore).
func (e *Engine) Restore(entries []Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.memtable = skiplist.New()
	for _, en := range entries {
		e.memtable.Put(en.Key, en.Value, en.Version)
	}
	return nil
}

func (e *Engine) flushLoop() {
	defer close(e.doneCh)
	for {
		select {
		case <-e.flushCh:
			if err := e.flush(); err != nil {
				log.Error().Err(err).Msg("memtable flush error")
			}
		case <-e.stopCh:
			_ = e.flush()
			return
		}
	}
}

func (e *Engine) flush() error {
	e.mu.Lock()
	if e.memtable.Len() == 0 {
		e.mu.Unlock()
		return nil
	}
	e.immutable = e.memtable
	e.memtable = skiplist.New()
	e.mu.Unlock()

	path := filepath.Join(e.dir, "sst", fmt.Sprintf("%d.sst", time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	w, err := sstable.NewWriter(path)
	if err != nil {
		return err
	}

	entries := e.immutable.All()
	for _, se := range entries {
		if err := w.Write(sstable.Entry{
			Key:     se.Key,
			Value:   se.Value,
			Version: se.Version,
			Deleted: se.Deleted,
		}); err != nil {
			w.Close()
			os.Remove(path)
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	e.compact.Register(path)

	e.mu.Lock()
	e.immutable = nil
	e.mu.Unlock()

	log.Info().Str("path", path).Int("entries", len(entries)).Msg("memtable flushed to SSTable")
	return nil
}

// Close gracefully shuts down the engine.
func (e *Engine) Close() error {
	close(e.stopCh)
	<-e.doneCh
	e.compact.Stop()
	return e.wal.Close()
}

func toEntry(se skiplist.Entry) Entry {
	return Entry{
		Key:     se.Key,
		Value:   se.Value,
		Version: se.Version,
		Deleted: se.Deleted,
	}
}
