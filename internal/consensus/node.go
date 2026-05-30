package consensus

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/config"
	"github.com/makhskham/oncloudkv/internal/storage"
	"github.com/makhskham/oncloudkv/internal/watch"
)

const applyTimeout = 5 * time.Second

// Node wraps a hashicorp/raft instance with our FSM, transport, and stores.
type Node struct {
	ID     string
	Raft   *raft.Raft
	FSM    *FSM
	Batch  *BatchWriter
	Config *config.Config
}

// NewNode constructs and starts a Raft node.
func NewNode(cfg *config.Config, store *storage.Engine, hub *watch.Hub) (*Node, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.Node.ID)
	raftCfg.HeartbeatTimeout = cfg.Raft.HeartbeatTimeout
	raftCfg.ElectionTimeout = cfg.Raft.ElectionTimeout
	raftCfg.SnapshotThreshold = cfg.Raft.SnapshotThreshold

	if err := os.MkdirAll(cfg.Raft.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft datadir: %w", err)
	}

	// stable store and log store backed by BoltDB
	boltPath := filepath.Join(cfg.Raft.DataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("raft boltdb: %w", err)
	}

	// snapshot store
	snapDir := filepath.Join(cfg.Raft.DataDir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft snapshots: %w", err)
	}

	// TCP transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.Raft.Addr)
	if err != nil {
		return nil, fmt.Errorf("raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.Raft.Addr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}

	fsm := NewFSM(store, hub)

	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raft new: %w", err)
	}

	// bootstrap single-node or join existing cluster
	if cfg.Raft.BootstrapCluster {
		servers := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftCfg.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		for _, peer := range cfg.Raft.Peers {
			servers.Servers = append(servers.Servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.Addr),
			})
		}
		f := r.BootstrapCluster(servers)
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("raft bootstrap: %w", err)
		}
	}

	log.Info().Str("id", cfg.Node.ID).Str("raft_addr", cfg.Raft.Addr).Msg("Raft node started")

	batch := NewBatchWriter(r, applyTimeout)

	return &Node{
		ID:     cfg.Node.ID,
		Raft:   r,
		FSM:    fsm,
		Batch:  batch,
		Config: cfg,
	}, nil
}

// IsLeader reports whether this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader (empty if unknown).
func (n *Node) LeaderAddr() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a new voting member to the cluster (must be called on leader).
func (n *Node) AddVoter(id, addr string) error {
	f := n.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// AddLearner adds a non-voting learner node (read replica).
func (n *Node) AddLearner(id, addr string) error {
	f := n.Raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// Stats returns Raft internal statistics for the metrics layer.
func (n *Node) Stats() map[string]string {
	return n.Raft.Stats()
}

// Shutdown cleanly stops the Raft node and batch writer.
func (n *Node) Shutdown() error {
	n.Batch.Stop()
	f := n.Raft.Shutdown()
	return f.Error()
}
