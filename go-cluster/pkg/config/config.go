// Package config provides configuration for the cluster
package config

import (
	"os"

	"github.com/spf13/viper"
)

// Config holds all configuration for a cluster node
type Config struct {
	// Node identification
	NodeID  string `mapstructure:"node_id"`
	DataDir string `mapstructure:"data_dir"`

	// Network addresses
	HTTPAddr string `mapstructure:"http_addr"`
	GRPCAddr string `mapstructure:"grpc_addr"`
	RaftAddr string `mapstructure:"raft_addr"`

	// Cluster settings
	NumShards         int `mapstructure:"num_shards"`
	ReplicationFactor int `mapstructure:"replication_factor"`

	// Storage settings
	MemtableSize   int64 `mapstructure:"memtable_size"`
	BlockCacheSize int64 `mapstructure:"block_cache_size"`
	WALEnabled     bool  `mapstructure:"wal_enabled"`

	// Performance tuning
	MaxConnections int `mapstructure:"max_connections"`
	ReadTimeout    int `mapstructure:"read_timeout_ms"`
	WriteTimeout   int `mapstructure:"write_timeout_ms"`

	// Rust core connection
	RustCoreSocket string `mapstructure:"rust_core_socket"`

	// Python AI service
	PythonAIEndpoint string `mapstructure:"python_ai_endpoint"`

	// Multi-Tier Storage Policies
	Tiering TieringConfig `mapstructure:"tiering" json:"tiering"`
}

// TieringConfig holds configuration for storage tiers
type TieringConfig struct {
	HotPolicy  TierPolicy `mapstructure:"hot_policy" json:"hot_policy"`
	WarmPolicy TierPolicy `mapstructure:"warm_policy" json:"warm_policy"`
	ColdPolicy TierPolicy `mapstructure:"cold_policy" json:"cold_policy"`
}

// TierPolicy defines redundancy strategy for a tier
type TierPolicy struct {
	Enabled  bool               `mapstructure:"enabled" json:"enabled"`
	Strategy RedundancyStrategy `mapstructure:"strategy" json:"strategy"`
}

// RedundancyStrategy fields
type RedundancyStrategy struct {
	Type         string `mapstructure:"type" json:"type"` // "Replication" or "ErasureCoding"
	Factor       int    `mapstructure:"factor,omitempty" json:"factor,omitempty"`
	DataShards   int    `mapstructure:"data_shards,omitempty" json:"data_shards,omitempty"`
	ParityShards int    `mapstructure:"parity_shards,omitempty" json:"parity_shards,omitempty"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		NodeID:            hostname,
		DataDir:           "./data",
		HTTPAddr:          ":8080",
		GRPCAddr:          ":9090",
		RaftAddr:          ":10000",
		NumShards:         16,
		ReplicationFactor: 3,
		MemtableSize:      64 * 1024 * 1024,  // 64MB
		BlockCacheSize:    128 * 1024 * 1024, // 128MB
		WALEnabled:        true,
		MaxConnections:    1000,
		ReadTimeout:       5000,
		WriteTimeout:      10000,
		RustCoreSocket:    "/tmp/tdb-core.sock",
		PythonAIEndpoint:  "http://localhost:8000",
		Tiering: TieringConfig{
			HotPolicy: TierPolicy{
				Enabled: true,
				Strategy: RedundancyStrategy{
					Type:   "Replication",
					Factor: 1,
				},
			},
			WarmPolicy: TierPolicy{
				Enabled: false, // User can enable via config
				Strategy: RedundancyStrategy{
					Type:         "ErasureCoding",
					DataShards:   6,
					ParityShards: 3,
				},
			},
			ColdPolicy: TierPolicy{
				Enabled: false,
				Strategy: RedundancyStrategy{
					Type:         "ErasureCoding",
					DataShards:   16,
					ParityShards: 4,
				},
			},
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	cfg := DefaultConfig()
	if err := viper.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Add validation logic
	return nil
}
