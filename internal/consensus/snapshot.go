package consensus

import (
	"encoding/json"

	"github.com/hashicorp/raft"

	"github.com/makhskham/oncloudkv/internal/storage"
)

type fsmSnapshot struct {
	entries []storage.Entry
}

// Persist writes the snapshot to the provided sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.entries)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is a no-op - no resources to free.
func (s *fsmSnapshot) Release() {}
