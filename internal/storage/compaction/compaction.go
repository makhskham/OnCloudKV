// Package compaction merges SSTables in the background to reclaim space
// and maintain read performance. Uses size-tiered compaction strategy.
package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/storage/sstable"
)

const (
	minCompactTables = 4    // compact when this many same-tier tables exist
	sizeTierFactor   = 10.0 // tables within 10x size are in the same tier
)

// Compactor manages background SSTable compaction.
type Compactor struct {
	dir      string
	interval time.Duration
	mu       sync.Mutex
	tables   []string // current SSTable paths (newest first)
	stop     chan struct{}
	done     chan struct{}
}

// New creates a Compactor that scans dir every interval.
func New(dir string, interval time.Duration) *Compactor {
	return &Compactor{
		dir:      dir,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Register adds an SSTable path to the compaction candidate list.
func (c *Compactor) Register(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = append([]string{path}, c.tables...) // prepend (newest first)
}

// Start runs the compaction loop in a background goroutine.
func (c *Compactor) Start() {
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.compact(); err != nil {
					log.Error().Err(err).Msg("compaction error")
				}
			case <-c.stop:
				return
			}
		}
	}()
}

// Stop halts the compaction loop and waits for it to exit.
func (c *Compactor) Stop() {
	close(c.stop)
	<-c.done
}

// Tables returns the current ordered list of SSTable paths (newest first).
func (c *Compactor) Tables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.tables))
	copy(out, c.tables)
	return out
}

func (c *Compactor) compact() error {
	c.mu.Lock()
	tables := make([]string, len(c.tables))
	copy(tables, c.tables)
	c.mu.Unlock()

	if len(tables) < minCompactTables {
		return nil
	}

	// group by size tier
	tier := c.selectTier(tables)
	if len(tier) < minCompactTables {
		return nil
	}

	log.Info().Int("count", len(tier)).Msg("starting compaction")
	merged, err := c.merge(tier)
	if err != nil {
		return err
	}

	// swap out old tables, insert merged table
	c.mu.Lock()
	tierSet := make(map[string]bool, len(tier))
	for _, t := range tier {
		tierSet[t] = true
	}
	var kept []string
	for _, t := range c.tables {
		if !tierSet[t] {
			kept = append(kept, t)
		}
	}
	c.tables = append([]string{merged}, kept...)
	c.mu.Unlock()

	// remove old files
	for _, t := range tier {
		os.Remove(t)
	}
	log.Info().Str("output", merged).Msg("compaction complete")
	return nil
}

func (c *Compactor) selectTier(tables []string) []string {
	type sizedTable struct {
		path string
		size int64
	}
	var sized []sizedTable
	for _, t := range tables {
		info, err := os.Stat(t)
		if err != nil {
			continue
		}
		sized = append(sized, sizedTable{t, info.Size()})
	}
	if len(sized) == 0 {
		return nil
	}
	sort.Slice(sized, func(i, j int) bool { return sized[i].size < sized[j].size })

	// find the largest group within sizeTierFactor
	best := []string{sized[0].path}
	for i := 1; i < len(sized); i++ {
		ratio := float64(sized[i].size) / float64(sized[0].size)
		if ratio <= sizeTierFactor {
			best = append(best, sized[i].path)
		}
	}
	return best
}

func (c *Compactor) merge(paths []string) (string, error) {
	var readers []*sstable.Reader
	for _, p := range paths {
		r, err := sstable.OpenReader(p)
		if err != nil {
			for _, rd := range readers {
				rd.Close()
			}
			return "", err
		}
		readers = append(readers, r)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	outPath := filepath.Join(c.dir, fmt.Sprintf("compact-%d.sst", time.Now().UnixNano()))
	w, err := sstable.NewWriter(outPath)
	if err != nil {
		return "", err
	}

	// k-way merge using a simple heap approach - collect all entries, deduplicate
	// keeping the highest version per key, then write sorted.
	type versionedEntry struct {
		e   sstable.Entry
		ver int64
	}
	seen := make(map[string]versionedEntry)

	for _, r := range readers {
		entries, err := r.Scan("", 0)
		if err != nil {
			w.Close()
			os.Remove(outPath)
			return "", err
		}
		for _, e := range entries {
			if prev, ok := seen[e.Key]; !ok || e.Version > prev.ver {
				seen[e.Key] = versionedEntry{e, e.Version}
			}
		}
	}

	// collect and sort
	merged := make([]sstable.Entry, 0, len(seen))
	for _, ve := range seen {
		merged = append(merged, ve.e)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })

	for _, e := range merged {
		if e.Deleted {
			continue // drop tombstones during compaction (GC)
		}
		if err := w.Write(e); err != nil {
			w.Close()
			os.Remove(outPath)
			return "", err
		}
	}
	return outPath, w.Close()
}
