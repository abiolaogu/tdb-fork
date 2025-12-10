// Package config provides configuration for the cluster
package config

import (
	"os"

	"github.com/spf13/viper"
)

// Config holds all configuration for a cluster node
type Config struct {
	// Node identification
	NodeID   string `mapstructure:"node_id"`
	DataDir  string `mapstructure:"data_dir"`

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
