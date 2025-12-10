// Package cluster implements distributed cluster coordination
// using Raft consensus for consistency and high availability.
package cluster

import (
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
	"github.com/tdb-plus/cluster/pkg/config"
	"go.uber.org/zap"
)

// Node represents a cluster node with Raft consensus
type Node struct {
	config    *config.Config
	logger    *zap.Logger
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport

	// Cluster membership
	peers    map[string]string // nodeID -> address
	peersMu  sync.RWMutex

	// Node state
	isLeader   bool
	leaderAddr string
	leaderMu   sync.RWMutex

	// Shard assignments
	shards    map[uint32]*ShardInfo
	shardsMu  sync.RWMutex
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

	node := &Node{
		config: cfg,
		logger: logger,
		peers:  make(map[string]string),
		shards: make(map[uint32]*ShardInfo),
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
	mu     sync.RWMutex
	data   map[string]map[string][]byte // collection -> key -> value
}

// NewFSM creates a new FSM
func NewFSM(node *Node, logger *zap.Logger) *FSM {
	return &FSM{
		node:   node,
		logger: logger,
		data:   make(map[string]map[string][]byte),
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("Failed to unmarshal command", zap.Error(err))
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case "set":
		if f.data[cmd.Collection] == nil {
			f.data[cmd.Collection] = make(map[string][]byte)
		}
		f.data[cmd.Collection][cmd.Key] = cmd.Value
	case "delete":
		if f.data[cmd.Collection] != nil {
			delete(f.data[cmd.Collection], cmd.Key)
		}
	}

	return nil
}

// Snapshot returns an FSM snapshot
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy the data
	data := make(map[string]map[string][]byte)
	for col, keys := range f.data {
		data[col] = make(map[string][]byte)
		for k, v := range keys {
			data[col][k] = v
		}
	}

	return &fsmSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string]map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}

	f.mu.Lock()
	f.data = data
	f.mu.Unlock()

	return nil
}

type fsmSnapshot struct {
	data map[string]map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(s.data)
		if err != nil {
			return err
		}
		if _, err := sink.Write(data); err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
