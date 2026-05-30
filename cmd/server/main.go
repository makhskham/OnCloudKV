package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/config"
	"github.com/makhskham/oncloudkv/internal/consensus"
	grpcapi "github.com/makhskham/oncloudkv/internal/api/grpc"
	httpapi "github.com/makhskham/oncloudkv/internal/api/http"
	"github.com/makhskham/oncloudkv/internal/membership"
	"github.com/makhskham/oncloudkv/internal/metrics"
	"github.com/makhskham/oncloudkv/internal/storage"
	"github.com/makhskham/oncloudkv/internal/watch"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal().Err(err).Str("config", cfgPath).Msg("failed to load config")
	}

	// storage engine
	store, err := storage.Open(cfg.Storage.DataDir, cfg.Storage.MemtableSizeMB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open storage engine")
	}
	defer store.Close()

	// watch hub (FSM event broadcast)
	hub := watch.NewHub()

	// raft consensus node
	node, err := consensus.NewNode(cfg, store, hub)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start Raft node")
	}
	defer node.Shutdown()

	// gossip cluster membership (optional - skipped if no gossip addr configured)
	if cfg.Node.Addr != "" {
		var seeds []string
		for _, p := range cfg.Raft.Peers {
			seeds = append(seeds, p.Addr)
		}
		if _, err := membership.Join(
			cfg.Node.ID,
			cfg.Node.Addr,
			cfg.Raft.Addr,
			cfg.GRPC.Addr,
			seeds,
		); err != nil {
			log.Warn().Err(err).Msg("gossip join failed - continuing without auto-discovery")
		}
	}

	// metrics server (non-blocking)
	go metrics.ServeHTTP(cfg.Metrics.Addr)

	// HTTP REST server (non-blocking)
	httpSrv := httpapi.New(node, store)
	go func() {
		if err := httpSrv.Listen(cfg.HTTP.Addr); err != nil {
			log.Error().Err(err).Msg("HTTP server error")
		}
	}()

	// gRPC server (non-blocking)
	grpcSrv := grpcapi.New(node, store, hub)
	go func() {
		if err := grpcSrv.Listen(cfg.GRPC.Addr); err != nil {
			log.Fatal().Err(err).Msg("gRPC server error")
		}
	}()

	log.Info().
		Str("node", cfg.Node.ID).
		Str("grpc", cfg.GRPC.Addr).
		Str("http", cfg.HTTP.Addr).
		Str("raft", cfg.Raft.Addr).
		Msg("OnCloudKV node ready")

	// wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down...")
}
