package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Node    NodeConfig
	Raft    RaftConfig
	Storage StorageConfig
	GRPC    GRPCConfig
	HTTP    HTTPConfig
	Metrics MetricsConfig
}

type NodeConfig struct {
	ID   string
	Addr string
}

type RaftConfig struct {
	Addr              string
	DataDir           string
	BootstrapCluster  bool
	Peers             []PeerConfig
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
}

type PeerConfig struct {
	ID   string
	Addr string
}

type StorageConfig struct {
	DataDir         string
	MemtableSizeMB  int64
	CompactionSecs  int
}

type GRPCConfig struct {
	Addr string
	TLS  TLSConfig
}

type HTTPConfig struct {
	Addr string
}

type MetricsConfig struct {
	Addr string
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	CAFile   string
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("OCKV")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	cfg := &Config{}
	cfg.Node.ID = v.GetString("node.id")
	cfg.Node.Addr = v.GetString("node.addr")

	cfg.Raft.Addr = v.GetString("raft.addr")
	cfg.Raft.DataDir = v.GetString("raft.data_dir")
	cfg.Raft.BootstrapCluster = v.GetBool("raft.bootstrap")
	cfg.Raft.HeartbeatTimeout = v.GetDuration("raft.heartbeat_timeout")
	cfg.Raft.ElectionTimeout = v.GetDuration("raft.election_timeout")
	cfg.Raft.SnapshotThreshold = uint64(v.GetInt64("raft.snapshot_threshold"))

	cfg.Storage.DataDir = v.GetString("storage.data_dir")
	cfg.Storage.MemtableSizeMB = v.GetInt64("storage.memtable_size_mb")
	cfg.Storage.CompactionSecs = v.GetInt("storage.compaction_interval_secs")

	cfg.GRPC.Addr = v.GetString("grpc.addr")
	cfg.GRPC.TLS.Enabled = v.GetBool("grpc.tls.enabled")
	cfg.GRPC.TLS.CertFile = v.GetString("grpc.tls.cert_file")
	cfg.GRPC.TLS.KeyFile = v.GetString("grpc.tls.key_file")
	cfg.GRPC.TLS.CAFile = v.GetString("grpc.tls.ca_file")

	cfg.HTTP.Addr = v.GetString("http.addr")
	cfg.Metrics.Addr = v.GetString("metrics.addr")

	// peers
	peersRaw := v.Get("raft.peers")
	if peers, ok := peersRaw.([]interface{}); ok {
		for _, p := range peers {
			if pm, ok := p.(map[string]interface{}); ok {
				cfg.Raft.Peers = append(cfg.Raft.Peers, PeerConfig{
					ID:   pm["id"].(string),
					Addr: pm["addr"].(string),
				})
			}
		}
	}

	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("node.addr", "0.0.0.0:7000")
	v.SetDefault("raft.addr", "0.0.0.0:7001")
	v.SetDefault("raft.data_dir", "/data/raft")
	v.SetDefault("raft.bootstrap", false)
	v.SetDefault("raft.heartbeat_timeout", "500ms")
	v.SetDefault("raft.election_timeout", "1s")
	v.SetDefault("raft.snapshot_threshold", 10000)
	v.SetDefault("storage.data_dir", "/data/kv")
	v.SetDefault("storage.memtable_size_mb", 64)
	v.SetDefault("storage.compaction_interval_secs", 30)
	v.SetDefault("grpc.addr", "0.0.0.0:7002")
	v.SetDefault("http.addr", "0.0.0.0:7003")
	v.SetDefault("metrics.addr", "0.0.0.0:7004")
}
