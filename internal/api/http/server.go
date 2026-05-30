// Package http implements the REST API gateway.
// All operations mirror the gRPC service.
// Consistency is controlled via the X-Consistency request header.
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/makhskham/oncloudkv/internal/consensus"
	"github.com/makhskham/oncloudkv/internal/consistency"
	"github.com/makhskham/oncloudkv/internal/metrics"
	"github.com/makhskham/oncloudkv/internal/storage"
)

// Server is the HTTP REST gateway.
type Server struct {
	node   *consensus.Node
	store  *storage.Engine
	reader *consistency.Reader
}

// New creates a Server.
func New(node *consensus.Node, store *storage.Engine) *Server {
	return &Server{
		node:   node,
		store:  store,
		reader: consistency.NewReader(store, node.Raft),
	}
}

// Listen starts the HTTP server on addr.
func (s *Server) Listen(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/keys/", s.handleKey)
	mux.HandleFunc("/v1/scan", s.handleScan)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", s.handleReadyz)

	srv := &http.Server{
		Addr:         addr,
		Handler:      logMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	log.Info().Str("addr", addr).Msg("HTTP server listening")
	return srv.ListenAndServe()
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/v1/keys/"):]
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getKey(w, r, key)
	case http.MethodPut, http.MethodPost:
		s.putKey(w, r, key)
	case http.MethodDelete:
		s.deleteKey(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) getKey(w http.ResponseWriter, r *http.Request, key string) {
	start := time.Now()
	mode := consistency.Mode(r.Header.Get("X-Consistency"))
	if mode == "" {
		mode = consistency.Eventual
	}
	sessionIndex, _ := strconv.ParseInt(r.Header.Get("X-Session-Index"), 10, 64)
	minVersion, _ := strconv.ParseInt(r.Header.Get("X-Min-Version"), 10, 64)

	entry, found, err := s.reader.Get(r.Context(), key, mode, sessionIndex, minVersion)
	metrics.ObserveGet(start, string(mode))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Version", strconv.FormatInt(entry.Version, 10))
	w.Write(entry.Value)
}

func (s *Server) putKey(w http.ResponseWriter, r *http.Request, key string) {
	if !s.node.IsLeader() {
		w.Header().Set("X-Leader", s.node.LeaderAddr())
		http.Error(w, "not leader", http.StatusTemporaryRedirect)
		return
	}
	var body []byte
	buf := make([]byte, 4<<20) // 4 MiB max
	n, _ := r.Body.Read(buf)
	body = buf[:n]

	ttl, _ := strconv.ParseInt(r.URL.Query().Get("ttl"), 10, 64)
	start := time.Now()
	res := s.node.Batch.Submit(consensus.Command{
		Type:       consensus.CmdPut,
		Key:        key,
		Value:      body,
		TTLSeconds: ttl,
	})
	metrics.ObservePut(start)

	if res.Err != nil {
		http.Error(w, res.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Raft-Index", strconv.FormatInt(res.Version, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request, key string) {
	if !s.node.IsLeader() {
		w.Header().Set("X-Leader", s.node.LeaderAddr())
		http.Error(w, "not leader", http.StatusTemporaryRedirect)
		return
	}
	start := time.Now()
	res := s.node.Batch.Submit(consensus.Command{Type: consensus.CmdDelete, Key: key})
	metrics.ObserveDelete(start)

	if res.Err != nil {
		http.Error(w, res.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Raft-Index", strconv.FormatInt(res.Version, 10))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries := s.store.Scan(prefix, limit)

	type item struct {
		Key     string `json:"key"`
		Value   []byte `json:"value"`
		Version int64  `json:"version"`
	}
	items := make([]item, len(entries))
	for i, e := range entries {
		items[i] = item{Key: e.Key, Value: e.Value, Version: e.Version}
	}
	writeJSON(w, items)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.node.Stats()
	writeJSON(w, stats)
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.node.Raft.AppliedIndex() == 0 {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok"))
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Debug().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Dur("latency", time.Since(start)).
			Msg("HTTP")
	})
}

// contextKey avoids collisions in context.WithValue.
type contextKey struct{}

func withContext(ctx context.Context, key contextKey, val interface{}) context.Context {
	return context.WithValue(ctx, key, val)
}
