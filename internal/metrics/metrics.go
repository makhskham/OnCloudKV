// Package metrics exposes Prometheus metrics for the OnCloudKV node.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	PutLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "oncloudkv_put_duration_seconds",
		Help:    "Latency of Put operations end-to-end (client to Raft commit)",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	})

	GetLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "oncloudkv_get_duration_seconds",
		Help:    "Latency of Get operations by consistency mode",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 14),
	}, []string{"consistency"})

	DeleteLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "oncloudkv_delete_duration_seconds",
		Help:    "Latency of Delete operations end-to-end",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	})

	RaftCommitIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "oncloudkv_raft_commit_index",
		Help: "Current Raft commit index",
	})

	RaftAppliedIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "oncloudkv_raft_applied_index",
		Help: "Last applied Raft log index",
	})

	RaftTerm = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "oncloudkv_raft_term",
		Help: "Current Raft term",
	})

	RaftIsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "oncloudkv_raft_is_leader",
		Help: "1 if this node is the Raft leader, 0 otherwise",
	})

	ActiveWatchers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "oncloudkv_active_watchers",
		Help: "Number of active Watch stream subscriptions",
	})

	RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "oncloudkv_requests_total",
		Help: "Total number of requests by operation and status",
	}, []string{"op", "status"})
)

func init() {
	prometheus.MustRegister(
		PutLatency, GetLatency, DeleteLatency,
		RaftCommitIndex, RaftAppliedIndex, RaftTerm, RaftIsLeader,
		ActiveWatchers, RequestsTotal,
	)
}

// ObservePut records a Put operation duration.
func ObservePut(start time.Time) {
	PutLatency.Observe(time.Since(start).Seconds())
}

// ObserveGet records a Get operation duration with its consistency mode.
func ObserveGet(start time.Time, consistency string) {
	GetLatency.WithLabelValues(consistency).Observe(time.Since(start).Seconds())
}

// ObserveDelete records a Delete operation duration.
func ObserveDelete(start time.Time) {
	DeleteLatency.Observe(time.Since(start).Seconds())
}

// ServeHTTP starts the Prometheus metrics endpoint on addr.
func ServeHTTP(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	log.Info().Str("addr", addr).Msg("Metrics server listening")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal().Err(err).Msg("Metrics server failed")
	}
}
