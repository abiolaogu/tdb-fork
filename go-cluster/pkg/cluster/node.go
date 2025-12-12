// Package cluster implements distributed cluster coordination
// using Raft consensus for consistency and high availability.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/lumadb/cluster/pkg/config"
	"github.com/lumadb/cluster/pkg/core" // New import
	"github.com/lumadb/cluster/pkg/platform/events"
	"go.uber.org/zap"
)

// Node represents a cluster node with Raft consensus
type Node struct {
	config    *config.Config
	logger    *zap.Logger
	db        *core.Database // Persistent storage
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport

	// Cluster membership
	peers   map[string]string // nodeID -> address
	peersMu sync.RWMutex

	// Node state
	isLeader   bool
	leaderAddr string
	leaderMu   sync.RWMutex

	// Shard assignments
	shards   map[uint32]*ShardInfo
	shardsMu sync.RWMutex

	// Event Triggers
	triggers *events.TriggerManager
}

// ShardInfo contains information about a shard
type ShardInfo struct {
	ID       uint32
	Leader   string
	Replicas []string
	Status   string
}

// NewNode creates a new cluster node
func NewNode(cfg *config.Config, logger *zap.Logger) (*Node, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// Initialize persistent storage (LumaDB Rust Core)
	// We need to pass configuration as a map
	// Simplest way: JSON roundtrip since core.Open marshals it anyway (slightly inefficient but fine for startup)
	var configMap map[string]interface{}
	cfgBytes, _ := json.Marshal(cfg)
	_ = json.Unmarshal(cfgBytes, &configMap)

	db, err := core.Open(filepath.Join(cfg.DataDir, "luma_data"), configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage engine: %w", err)
	}

	node := &Node{
		config:   cfg,
		logger:   logger,
		db:       db,
		peers:    make(map[string]string),
		shards:   make(map[uint32]*ShardInfo),
		triggers: events.NewTriggerManager(logger, cfg.RedpandaAddr),
	}

	// Initialize shards
	numShards := uint32(cfg.NumShards)
	if numShards == 0 {
		numShards = 16 // Default if 0
	}
	for i := uint32(0); i < numShards; i++ {
		node.shards[i] = &ShardInfo{
			ID:     i,
			Status: "active",
		}
	}

	// Create FSM
	node.fsm = NewFSM(node, logger)

	// Setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.MaxAppendEntries = 64
	raftConfig.SnapshotInterval = 120 * time.Second
	raftConfig.SnapshotThreshold = 8192

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}
	node.transport = transport

	// Create stores
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft instance
	ra, err := raft.NewRaft(raftConfig, node.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	node.raft = ra

	// Start leader monitoring
	go node.monitorLeadership()

	return node, nil
}

// UpdateShardStatus updates the status of a shard
func (n *Node) UpdateShardStatus(shardID uint32, leader string, status string) {
	n.shardsMu.Lock()
	defer n.shardsMu.Unlock()

	if shard, ok := n.shards[shardID]; ok {
		shard.Leader = leader
		shard.Status = status
	}
}

// Bootstrap starts a new cluster with this node as the initial leader
func (n *Node) Bootstrap() error {
	n.logger.Info("Bootstrapping new cluster")

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.config.NodeID),
				Address: raft.ServerAddress(n.config.RaftAddr),
			},
		},
	}

	future := n.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		if err != raft.ErrCantBootstrap {
			return fmt.Errorf("failed to bootstrap: %w", err)
		}
		n.logger.Info("Cluster already bootstrapped")
	}

	return nil
}

// Join joins an existing cluster
func (n *Node) Join(leaderAddr string) error {
	n.logger.Info("Joining cluster", zap.String("leader", leaderAddr))

	// TODO: Implement proper cluster join via RPC to leader
	// For now, this is a placeholder
	n.leaderMu.Lock()
	n.leaderAddr = leaderAddr
	n.leaderMu.Unlock()

	return nil
}

// Shutdown gracefully shuts down the node
func (n *Node) Shutdown() error {
	n.logger.Info("Shutting down node")

	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			return fmt.Errorf("raft shutdown failed: %w", err)
		}
	}

	if n.db != nil {
		if err := n.db.Close(); err != nil {
			n.logger.Error("Failed to close database", zap.Error(err))
		}
	}

	if n.triggers != nil {
		n.triggers.Close()
	}

	return nil
}

// IsLeader returns true if this node is the cluster leader
func (n *Node) IsLeader() bool {
	n.leaderMu.RLock()
	defer n.leaderMu.RUnlock()
	return n.isLeader
}

// LeaderAddr returns the address of the current leader
func (n *Node) LeaderAddr() string {
	n.leaderMu.RLock()
	defer n.leaderMu.RUnlock()
	return n.leaderAddr
}

// Apply applies a command to the Raft log
func (n *Node) Apply(cmd *Command, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader, leader is at %s", n.LeaderAddr())
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}

// GetPeers returns the current cluster peers
func (n *Node) GetPeers() map[string]string {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()

	peers := make(map[string]string)
	for k, v := range n.peers {
		peers[k] = v
	}
	return peers
}

// GetShards returns shard information
func (n *Node) GetShards() map[uint32]*ShardInfo {
	n.shardsMu.RLock()
	defer n.shardsMu.RUnlock()

	shards := make(map[uint32]*ShardInfo)
	for k, v := range n.shards {
		shards[k] = v
	}
	return shards
}

// GetShardForKey returns the shard responsible for a key
func (n *Node) GetShardForKey(key []byte) *ShardInfo {
	// Simple consistent hashing
	hash := fnv1a(key)
	shardID := uint32(hash % uint64(n.config.NumShards))

	n.shardsMu.RLock()
	defer n.shardsMu.RUnlock()

	return n.shards[shardID]
}

// GetDatabase returns the underlying database instance
func (n *Node) GetDatabase() *core.Database {
	return n.db
}

// GetConfig returns the node configuration
func (n *Node) GetConfig() *config.Config {
	return n.config
}

func (n *Node) monitorLeadership() {
	for {
		select {
		case isLeader := <-n.raft.LeaderCh():
			n.leaderMu.Lock()
			n.isLeader = isLeader
			if isLeader {
				n.logger.Info("This node is now the leader")
				n.leaderAddr = n.config.RaftAddr
			} else {
				addr, _ := n.raft.LeaderWithID()
				n.leaderAddr = string(addr)
				n.logger.Info("Leader changed", zap.String("new_leader", n.leaderAddr))
			}
			n.leaderMu.Unlock()
		}
	}
}

// FNV-1a hash function
func fnv1a(data []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

// Command represents a Raft command
type Command struct {
	Op         string          `json:"op"`
	Collection string          `json:"collection"`
	Key        string          `json:"key"`
	Value      json.RawMessage `json:"value,omitempty"`
}

// FSM is the Finite State Machine for Raft
type FSM struct {
	node   *Node
	logger *zap.Logger
}

// NewFSM creates a new FSM
func NewFSM(node *Node, logger *zap.Logger) *FSM {
	return &FSM{
		node:   node,
		logger: logger,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("Failed to unmarshal command", zap.Error(err))
		return err
	}

	switch cmd.Op {
	case "set":
		// Write to persistent Rust storage
		if _, err := f.node.db.Insert(cmd.Collection, cmd.Value); err != nil {
			f.logger.Error("Failed to insert into DB", zap.Error(err))
			return err
		}
	case "delete":
		if err := f.node.db.Delete(cmd.Collection, cmd.Key); err != nil {
			f.logger.Error("Failed to delete from DB", zap.Error(err))
			return err
		}
	}

	return nil
}

// Snapshot returns an FSM snapshot
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{node: f.node}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Create temp file for restore
	tmpFile, err := os.CreateTemp("", "luma-snapshot-*.bin")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy snapshot data to temp file
	if _, err := io.Copy(tmpFile, rc); err != nil {
		return fmt.Errorf("failed to copy snapshot data: %w", err)
	}

	// Restore DB from file
	f.logger.Info("Restoring from snapshot", zap.String("path", tmpFile.Name()))
	if err := f.node.db.Restore(tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to restore db: %w", err)
	}

	return nil
}

type fsmSnapshot struct {
	node *Node
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// Create temp file for snapshot
	tmpFile, err := os.CreateTemp("", "luma-snapshot-*.bin")
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close() // Close immediately, DB will open it

	// Create snapshot in temp file
	s.node.logger.Info("Creating snapshot", zap.String("path", tmpFile.Name()))
	if err := s.node.db.Snapshot(tmpFile.Name()); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to snapshot db: %w", err)
	}

	// Copy temp file to sink
	f, err := os.Open(tmpFile.Name())
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(sink, f); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to copy snapshot to sink: %w", err)
	}

	return nil
}

func (s *fsmSnapshot) Release() {}

// ListCollections returns all collection names
func (n *Node) ListCollections() ([]string, error) {
	if n.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	return n.db.ListCollections()
}

// GetDocument retrieves a document
func (n *Node) GetDocument(collection, id string) (map[string]interface{}, error) {
	if n.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	// TODO: Forward to leader if not leader?
	return n.db.Get(collection, id)
}

// RunQuery executes a query
func (n *Node) RunQuery(collection string, query interface{}) ([]map[string]interface{}, error) {
	if n.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	return n.db.Query(collection, query)
}

// InsertDocument inserts a document
func (n *Node) InsertDocument(collection string, doc map[string]interface{}) (string, error) {
	if n.raft == nil {
		// Fallback for non-raft mode tests
		if n.db == nil {
			return "", fmt.Errorf("database not initialized")
		}
		return n.db.Insert(collection, doc)
	}

	// Replicate via Raft
	// We need to marshal the doc to bytes
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	cmd := &Command{
		Op:         "set",
		Collection: collection,
		Value:      docBytes,
		// Key generation needs to happen here or in Apply
		// For simplicity, we assume ID is in doc or generated by DB.
		// If generated by DB, we might have issue with Raft deterministic playback if ID generation is non-deterministic.
		// Ideally, we generate ID here.
	}

	// Check if ID exists
	if id, ok := doc["_id"].(string); ok {
		cmd.Key = id
	} else {
		// Generate ID
		cmd.Key = fmt.Sprintf("%d", time.Now().UnixNano()) // Simple ID for now
		doc["_id"] = cmd.Key
		// Remarshal with ID
		docBytes, _ = json.Marshal(doc)
		cmd.Value = docBytes
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}

	future := n.raft.Apply(cmdBytes, 5*time.Second)
	if err := future.Error(); err != nil {
		return "", err
	}

	// Apply returns err or result from FSM.Apply
	// Our FSM.Apply returns error or nil
	resp := future.Response()
	if resp != nil {
		if err, ok := resp.(error); ok {
			return "", err
		}
	}

	// Fire AfterInsert Event
	// Note: We only fire if we are the leader (or in non-raft mode) to avoid duplicate events
	// If Raft is used, this code runs on the leader.

	// We fire asynchronously
	go n.triggers.Fire(context.Background(), collection, events.EventInsert, doc, nil)

	return cmd.Key, nil
}

// UpdateDocument updates a document
func (n *Node) UpdateDocument(collection, id string, updates map[string]interface{}) error {
	if n.db == nil {
		return fmt.Errorf("database not initialized")
	}
	// TODO: Raft replication
	return n.db.Update(collection, id, updates)
}

// DeleteDocument deletes a document
func (n *Node) DeleteDocument(collection, id string) error {
	if n.raft == nil {
		if n.db == nil {
			return fmt.Errorf("database not initialized")
		}
		return n.db.Delete(collection, id)
	}

	cmd := &Command{
		Op:         "delete",
		Collection: collection,
		Key:        id,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := n.raft.Apply(cmdBytes, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	resp := future.Response()
	if resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}
