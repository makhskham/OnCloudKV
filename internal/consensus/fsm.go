// Package consensus wires hashicorp/raft to the LSM storage engine.
package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/storage"
	"github.com/makhskham/oncloudkv/internal/watch"
)

// CommandType identifies the operation encoded in a Raft log entry.
type CommandType byte

const (
	CmdPut    CommandType = 0
	CmdDelete CommandType = 1
)

// Command is the payload serialised into every Raft log entry.
type Command struct {
	Type       CommandType `json:"t"`
	Key        string      `json:"k"`
	Value      []byte      `json:"v,omitempty"`
	TTLSeconds int64       `json:"ttl,omitempty"`
}

// FSM implements raft.FSM using the LSM storage engine as its state machine.
// The Raft log index is used as the MVCC version - identical to KV Fabric's
// design - which eliminates distributed ID generation entirely.
type FSM struct {
	mu      sync.RWMutex
	store   *storage.Engine
	watcher *watch.Hub
}

// NewFSM constructs an FSM backed by the given engine and watch hub.
func NewFSM(store *storage.Engine, watcher *watch.Hub) *FSM {
	return &FSM{store: store, watcher: watcher}
}

// Apply is called by Raft once a log entry is committed to a majority of nodes.
func (f *FSM) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		return nil
	}

	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Error().Err(err).Msg("FSM: failed to unmarshal command")
		return err
	}

	version := int64(l.Index)

	switch cmd.Type {
	case CmdPut:
		if err := f.store.Put(cmd.Key, cmd.Value, version); err != nil {
			log.Error().Err(err).Str("key", cmd.Key).Msg("FSM: put failed")
			return err
		}
		f.watcher.Notify(watch.Event{
			Type:    watch.EventPut,
			Key:     cmd.Key,
			Value:   cmd.Value,
			Version: version,
		})
	case CmdDelete:
		if err := f.store.Delete(cmd.Key, version); err != nil {
			log.Error().Err(err).Str("key", cmd.Key).Msg("FSM: delete failed")
			return err
		}
		f.watcher.Notify(watch.Event{
			Type:    watch.EventDelete,
			Key:     cmd.Key,
			Version: version,
		})
	default:
		return fmt.Errorf("FSM: unknown command type %d", cmd.Type)
	}

	return &ApplyResult{Version: version}
}

// ApplyResult is returned from Apply and surfaced to the caller via raft.ApplyFuture.
type ApplyResult struct {
	Version int64
}

// Snapshot returns a point-in-time snapshot of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	entries := f.store.Snapshot()
	f.mu.RUnlock()
	return &fsmSnapshot{entries: entries}, nil
}

// Restore resets the FSM to the state encoded in the snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	dec := json.NewDecoder(rc)
	var entries []storage.Entry
	if err := dec.Decode(&entries); err != nil {
		return fmt.Errorf("FSM restore: %w", err)
	}
	log.Info().Int("entries", len(entries)).Msg("FSM: restoring from snapshot")
	return f.store.Restore(entries)
}
