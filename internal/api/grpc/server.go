// Package grpc implements the KVService gRPC server.
package grpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/makhskham/oncloudkv/internal/consensus"
	"github.com/makhskham/oncloudkv/internal/consistency"
	"github.com/makhskham/oncloudkv/internal/metrics"
	"github.com/makhskham/oncloudkv/internal/storage"
	"github.com/makhskham/oncloudkv/internal/watch"
	pb "github.com/makhskham/oncloudkv/proto/gen"
)

// Server is the gRPC KVService implementation.
type Server struct {
	pb.UnimplementedKVServiceServer
	node    *consensus.Node
	store   *storage.Engine
	reader  *consistency.Reader
	hub     *watch.Hub
}

// New constructs a Server.
func New(node *consensus.Node, store *storage.Engine, hub *watch.Hub) *Server {
	return &Server{
		node:   node,
		store:  store,
		reader: consistency.NewReader(store, node.Raft),
		hub:    hub,
	}
}

// Listen starts the gRPC server on addr and blocks until stopped.
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(loggingInterceptor))
	pb.RegisterKVServiceServer(srv, s)
	reflection.Register(srv)
	log.Info().Str("addr", addr).Msg("gRPC server listening")
	return srv.Serve(lis)
}

// ── RPC implementations ───────────────────────────────────────────────────────

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	defer metrics.ObservePut(start)

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.Unavailable, "not leader - contact %s", s.node.LeaderAddr())
	}

	res := s.node.Batch.Submit(consensus.Command{
		Type:       consensus.CmdPut,
		Key:        req.Key,
		Value:      req.Value,
		TTLSeconds: req.TtlSeconds,
	})
	if res.Err != nil {
		metrics.RequestsTotal.WithLabelValues("put", "error").Inc()
		return nil, status.Errorf(codes.Internal, "put failed: %v", res.Err)
	}

	metrics.RequestsTotal.WithLabelValues("put", "ok").Inc()
	return &pb.PutResponse{Success: true, RaftIndex: res.Version}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	mode := consistencyMode(ctx, req.Consistency)
	start := time.Now()
	defer metrics.ObserveGet(start, string(mode))

	entry, found, err := s.reader.Get(ctx, req.Key, mode, req.SessionIndex, req.MinVersion)
	if err != nil {
		metrics.RequestsTotal.WithLabelValues("get", "error").Inc()
		return nil, status.Errorf(codes.Internal, "get failed: %v", err)
	}

	metrics.RequestsTotal.WithLabelValues("get", "ok").Inc()
	return &pb.GetResponse{
		Found:   found,
		Value:   entry.Value,
		Version: entry.Version,
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer metrics.ObserveDelete(start)

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.Unavailable, "not leader - contact %s", s.node.LeaderAddr())
	}

	res := s.node.Batch.Submit(consensus.Command{
		Type: consensus.CmdDelete,
		Key:  req.Key,
	})
	if res.Err != nil {
		metrics.RequestsTotal.WithLabelValues("delete", "error").Inc()
		return nil, status.Errorf(codes.Internal, "delete failed: %v", res.Err)
	}

	metrics.RequestsTotal.WithLabelValues("delete", "ok").Inc()
	return &pb.DeleteResponse{Success: true, RaftIndex: res.Version}, nil
}

func (s *Server) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	entries := s.store.Scan(req.Prefix, int(req.Limit))
	items := make([]*pb.KeyValue, 0, len(entries))
	for _, e := range entries {
		items = append(items, &pb.KeyValue{
			Key:     e.Key,
			Value:   e.Value,
			Version: e.Version,
		})
	}
	metrics.RequestsTotal.WithLabelValues("scan", "ok").Inc()
	return &pb.ScanResponse{Items: items}, nil
}

func (s *Server) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	metrics.ActiveWatchers.Inc()
	defer metrics.ActiveWatchers.Dec()

	id, ch := s.hub.Subscribe(req.Prefix)
	defer s.hub.Unsubscribe(id)

	metrics.RequestsTotal.WithLabelValues("watch", "ok").Inc()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			pbType := pb.WatchEvent_PUT
			if evt.Type == watch.EventDelete {
				pbType = pb.WatchEvent_DELETE
			}
			if err := stream.Send(&pb.WatchEvent{
				Type:    pbType,
				Key:     evt.Key,
				Value:   evt.Value,
				Version: evt.Version,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	stats := s.node.Stats()
	commitIndex, _ := strconv.ParseInt(stats["commit_index"], 10, 64)
	appliedIndex, _ := strconv.ParseInt(stats["applied_index"], 10, 64)

	var peers []string
	for _, m := range s.node.Raft.GetConfiguration().Configuration().Servers {
		peers = append(peers, string(m.Address))
	}

	metrics.RaftCommitIndex.Set(float64(commitIndex))
	metrics.RaftAppliedIndex.Set(float64(appliedIndex))
	if s.node.IsLeader() {
		metrics.RaftIsLeader.Set(1)
	} else {
		metrics.RaftIsLeader.Set(0)
	}

	return &pb.StatusResponse{
		NodeId:       s.node.ID,
		Leader:       s.node.LeaderAddr(),
		State:        s.node.Raft.State().String(),
		CommitIndex:  commitIndex,
		AppliedIndex: appliedIndex,
		Peers:        peers,
	}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func consistencyMode(ctx context.Context, fromRequest string) consistency.Mode {
	if fromRequest != "" {
		return consistency.Mode(fromRequest)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vals := md.Get("x-consistency"); len(vals) > 0 {
			return consistency.Mode(vals[0])
		}
	}
	return consistency.Eventual
}

func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Debug().
		Str("method", info.FullMethod).
		Dur("latency", time.Since(start)).
		Err(err).
		Msg("gRPC")
	return resp, err
}
