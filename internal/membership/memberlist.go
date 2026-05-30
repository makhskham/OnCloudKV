// Package membership implements gossip-based cluster discovery
// using hashicorp/memberlist. Nodes broadcast their gRPC and Raft
// addresses so peers can locate each other without manual configuration.
package membership

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog/log"
)

// NodeMeta is embedded in every gossip broadcast.
type NodeMeta struct {
	NodeID   string `json:"id"`
	RaftAddr string `json:"raft"`
	GRPCAddr string `json:"grpc"`
}

// Cluster manages the gossip ring for this node.
type Cluster struct {
	list *memberlist.Memberlist
	meta NodeMeta
}

// Join creates or joins a gossip cluster.
// seeds are addresses of known peers (empty for the first node).
func Join(nodeID, gossipAddr, raftAddr, grpcAddr string, seeds []string) (*Cluster, error) {
	host, portStr, err := net.SplitHostPort(gossipAddr)
	if err != nil {
		return nil, fmt.Errorf("membership: invalid gossip addr: %w", err)
	}
	port, _ := strconv.Atoi(portStr)

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = nodeID
	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.AdvertiseAddr = host
	cfg.AdvertisePort = port
	cfg.LogOutput = noopLogger{}

	meta := NodeMeta{NodeID: nodeID, RaftAddr: raftAddr, GRPCAddr: grpcAddr}
	metaBytes, _ := json.Marshal(meta)

	cfg.Delegate = &delegate{meta: metaBytes}
	cfg.Events = &eventDelegate{}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("membership: create: %w", err)
	}

	if len(seeds) > 0 {
		if n, err := list.Join(seeds); err != nil {
			log.Warn().Err(err).Int("joined", n).Msg("membership: partial join")
		}
	}

	log.Info().Str("node", nodeID).Str("addr", gossipAddr).Msg("Gossip cluster joined")
	return &Cluster{list: list, meta: meta}, nil
}

// Members returns metadata for all currently live nodes.
func (c *Cluster) Members() []NodeMeta {
	var out []NodeMeta
	for _, m := range c.list.Members() {
		var nm NodeMeta
		if err := json.Unmarshal(m.Meta, &nm); err == nil {
			out = append(out, nm)
		}
	}
	return out
}

// Leave broadcasts a graceful departure and closes the cluster.
func (c *Cluster) Leave() error {
	if err := c.list.Leave(0); err != nil {
		return err
	}
	return c.list.Shutdown()
}

// delegate satisfies memberlist.Delegate for metadata broadcasting.
type delegate struct{ meta []byte }

func (d *delegate) NodeMeta(limit int) []byte      { return d.meta }
func (d *delegate) NotifyMsg([]byte)               {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *delegate) LocalState(join bool) []byte    { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

// eventDelegate logs cluster topology changes.
type eventDelegate struct{}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Info().Str("node", node.Name).Msg("membership: node joined")
}
func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Warn().Str("node", node.Name).Msg("membership: node left")
}
func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {}

// noopLogger discards memberlist internal logs (zerolog handles ours).
type noopLogger struct{}

func (noopLogger) Write(p []byte) (n int, err error) { return len(p), nil }
