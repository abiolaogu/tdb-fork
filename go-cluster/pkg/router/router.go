// Package router implements request routing and load balancing
// Inspired by YugabyteDB's query routing and ScyllaDB's shard-awareness
package router

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tdb-plus/cluster/pkg/cluster"
	"go.uber.org/zap"
)

// Router handles request routing to appropriate shards/nodes
type Router struct {
	node       *cluster.Node
	logger     *zap.Logger
	connPools  map[string]*ConnectionPool
	poolsMu    sync.RWMutex
	roundRobin uint64
}

// ConnectionPool manages connections to a node
type ConnectionPool struct {
	addr        string
	connections chan *Connection
	maxSize     int
	activeCount int32
}

// Connection represents a connection to a storage node
type Connection struct {
	addr    string
	created time.Time
	inUse   bool
}

// NewRouter creates a new router
func NewRouter(node *cluster.Node, logger *zap.Logger) *Router {
	return &Router{
		node:      node,
		logger:    logger,
		connPools: make(map[string]*ConnectionPool),
	}
}

// Route determines the target node for a request
func (r *Router) Route(ctx context.Context, collection string, key []byte) (string, error) {
	// Get shard for key
	shard := r.node.GetShardForKey(key)
	if shard == nil || shard.Leader == "" {
		// Fall back to local node
		return "localhost", nil
	}

	return shard.Leader, nil
}

// RouteRead routes a read request (can go to any replica)
func (r *Router) RouteRead(ctx context.Context, collection string, key []byte) (string, error) {
	shard := r.node.GetShardForKey(key)
	if shard == nil {
		return "localhost", nil
	}

	// Load balance across replicas
	replicas := append([]string{shard.Leader}, shard.Replicas...)
	if len(replicas) == 0 {
		return "localhost", nil
	}

	idx := atomic.AddUint64(&r.roundRobin, 1) % uint64(len(replicas))
	return replicas[idx], nil
}

// RouteWrite routes a write request (must go to leader)
func (r *Router) RouteWrite(ctx context.Context, collection string, key []byte) (string, error) {
	if !r.node.IsLeader() {
		return r.node.LeaderAddr(), nil
	}

	shard := r.node.GetShardForKey(key)
	if shard == nil || shard.Leader == "" {
		return "localhost", nil
	}

	return shard.Leader, nil
}

// GetConnection gets a connection from the pool
func (r *Router) GetConnection(addr string) (*Connection, error) {
	pool := r.getOrCreatePool(addr)

	select {
	case conn := <-pool.connections:
		return conn, nil
	default:
		// Create new connection if pool is empty
		return r.createConnection(addr)
	}
}

// ReleaseConnection returns a connection to the pool
func (r *Router) ReleaseConnection(conn *Connection) {
	r.poolsMu.RLock()
	pool, exists := r.connPools[conn.addr]
	r.poolsMu.RUnlock()

	if !exists {
		return
	}

	select {
	case pool.connections <- conn:
	default:
		// Pool is full, discard connection
	}
}

func (r *Router) getOrCreatePool(addr string) *ConnectionPool {
	r.poolsMu.RLock()
	pool, exists := r.connPools[addr]
	r.poolsMu.RUnlock()

	if exists {
		return pool
	}

	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	// Double check
	if pool, exists = r.connPools[addr]; exists {
		return pool
	}

	pool = &ConnectionPool{
		addr:        addr,
		connections: make(chan *Connection, 100),
		maxSize:     100,
	}
	r.connPools[addr] = pool
	return pool
}

func (r *Router) createConnection(addr string) (*Connection, error) {
	return &Connection{
		addr:    addr,
		created: time.Now(),
		inUse:   true,
	}, nil
}

// HealthCheck checks the health of a node
func (r *Router) HealthCheck(addr string) bool {
	// TODO: Implement actual health check
	return true
}

// GetClusterTopology returns the current cluster topology
func (r *Router) GetClusterTopology() map[string]interface{} {
	return map[string]interface{}{
		"leader":    r.node.LeaderAddr(),
		"is_leader": r.node.IsLeader(),
		"peers":     r.node.GetPeers(),
		"shards":    r.node.GetShards(),
	}
}
