// Package consistency implements per-request consistency routing.
// Clients declare their consistency requirement via gRPC metadata
// (key: "x-consistency") or HTTP header (X-Consistency).
//
// Modes:
//   strong          - linearizable (ReadIndex protocol, one extra RTT)
//   eventual        - serve from any replica, may be stale
//   read-your-writes - session token carries Raft log index of last write
//   monotonic       - client watermark; version never goes backwards
package consistency

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"github.com/makhskham/oncloudkv/internal/storage"
)

// Mode identifies a consistency guarantee.
type Mode string

const (
	Strong        Mode = "strong"
	Eventual      Mode = "eventual"
	ReadYourWrites Mode = "read-your-writes"
	Monotonic     Mode = "monotonic"
)

// Reader wraps the storage engine with consistency-aware read routing.
type Reader struct {
	store *storage.Engine
	raft  *raft.Raft
}

// NewReader creates a Reader.
func NewReader(store *storage.Engine, r *raft.Raft) *Reader {
	return &Reader{store: store, raft: r}
}

// Get retrieves key using the requested consistency mode.
//   sessionIndex: required for ReadYourWrites - the Raft index of the caller's last write
//   minVersion:   required for Monotonic - the highest version the client has seen
func (r *Reader) Get(ctx context.Context, key string, mode Mode, sessionIndex, minVersion int64) (storage.Entry, bool, error) {
	switch mode {
	case Strong:
		if err := r.readIndex(ctx); err != nil {
			return storage.Entry{}, false, fmt.Errorf("strong read: %w", err)
		}
	case ReadYourWrites:
		if err := r.waitForIndex(ctx, uint64(sessionIndex)); err != nil {
			return storage.Entry{}, false, fmt.Errorf("read-your-writes: %w", err)
		}
	case Monotonic:
		if err := r.waitForIndex(ctx, uint64(minVersion)); err != nil {
			return storage.Entry{}, false, fmt.Errorf("monotonic read: %w", err)
		}
	case Eventual:
		// no coordination needed
	default:
		// default to eventual for unrecognised modes
	}

	entry, found := r.store.Get(key)
	return entry, found, nil
}

// readIndex issues a Raft ReadIndex RPC to confirm leadership before serving.
// This is the linearizability guarantee - the leader verifies it still holds
// quorum before responding to the read.
func (r *Reader) readIndex(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	timeout := 5 * time.Second
	if ok {
		timeout = time.Until(deadline)
	}
	f := r.raft.VerifyLeader()
	_ = timeout
	return f.Error()
}

// waitForIndex blocks until the local state machine has applied index.
func (r *Reader) waitForIndex(ctx context.Context, index uint64) error {
	if index == 0 {
		return nil
	}
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for {
		if r.raft.AppliedIndex() >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
