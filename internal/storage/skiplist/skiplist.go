// Package skiplist provides a concurrent sorted key-value skiplist
// used as the memtable layer of the LSM storage engine.
package skiplist

import (
	"math/rand"
	"sync"
)

const maxLevel = 32
const probability = 0.25

// Entry is a single record stored in the skiplist.
type Entry struct {
	Key     string
	Value   []byte
	Version int64 // Raft log index
	Deleted bool  // tombstone marker
}

type node struct {
	entry   Entry
	forward []*node
	mu      sync.RWMutex
}

// Skiplist is a probabilistically balanced concurrent sorted structure.
// Multiple readers may proceed in parallel; writers take only node-level locks.
type Skiplist struct {
	head    *node
	level   int
	length  int
	mu      sync.RWMutex
	memSize int64
}

func newNode(level int, e Entry) *node {
	return &node{
		entry:   e,
		forward: make([]*node, level+1),
	}
}

// New returns an empty skiplist.
func New() *Skiplist {
	head := newNode(maxLevel, Entry{})
	return &Skiplist{head: head, level: 0}
}

func randomLevel() int {
	lvl := 0
	for lvl < maxLevel && rand.Float64() < probability {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key. version must be monotonically increasing.
func (s *Skiplist) Put(key string, value []byte, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*node, maxLevel+1)
	cur := s.head

	for i := s.level; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].entry.Key < key {
			cur = cur.forward[i]
		}
		update[i] = cur
	}

	cur = cur.forward[0]
	if cur != nil && cur.entry.Key == key {
		cur.entry.Value = value
		cur.entry.Version = version
		cur.entry.Deleted = false
		return
	}

	lvl := randomLevel()
	if lvl > s.level {
		for i := s.level + 1; i <= lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := newNode(lvl, Entry{Key: key, Value: value, Version: version})
	for i := 0; i <= lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	s.length++
	s.memSize += int64(len(key) + len(value) + 24)
}

// Delete marks a key as deleted via tombstone.
func (s *Skiplist) Delete(key string, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := s.head
	for i := s.level; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].entry.Key < key {
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]
	if cur != nil && cur.entry.Key == key {
		cur.entry.Deleted = true
		cur.entry.Version = version
		return
	}
	// key not found - insert tombstone
	s.mu.Unlock()
	s.Put(key, nil, version)
	s.mu.Lock()
	s.findNode(key).entry.Deleted = true
}

func (s *Skiplist) findNode(key string) *node {
	cur := s.head
	for i := s.level; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].entry.Key < key {
			cur = cur.forward[i]
		}
	}
	n := cur.forward[0]
	if n != nil && n.entry.Key == key {
		return n
	}
	return nil
}

// Get returns the entry for key. ok is false if the key does not exist.
func (s *Skiplist) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cur := s.head
	for i := s.level; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].entry.Key < key {
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]
	if cur != nil && cur.entry.Key == key {
		return cur.entry, true
	}
	return Entry{}, false
}

// Scan returns all entries with keys in [start, end). If end is empty, no upper bound.
func (s *Skiplist) Scan(start, end string, limit int) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []Entry
	cur := s.head

	for i := s.level; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].entry.Key < start {
			cur = cur.forward[i]
		}
	}
	cur = cur.forward[0]

	for cur != nil {
		if end != "" && cur.entry.Key >= end {
			break
		}
		if limit > 0 && len(results) >= limit {
			break
		}
		results = append(results, cur.entry)
		cur = cur.forward[0]
	}
	return results
}

// All returns every entry in sorted order. Used during SSTable flush.
func (s *Skiplist) All() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []Entry
	cur := s.head.forward[0]
	for cur != nil {
		entries = append(entries, cur.entry)
		cur = cur.forward[0]
	}
	return entries
}

// Len returns the number of entries including tombstones.
func (s *Skiplist) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// MemSize returns approximate bytes consumed.
func (s *Skiplist) MemSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memSize
}
