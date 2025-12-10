// Package pool provides connection pooling for stateless operations.
package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed     = errors.New("connection pool is closed")
	ErrPoolExhausted  = errors.New("connection pool exhausted")
	ErrConnectTimeout = errors.New("connection timeout")
)

// Connection represents a pooled connection
type Connection interface {
	Close() error
	IsValid() bool
	Reset() error
}

// ConnectionFactory creates new connections
type ConnectionFactory func(ctx context.Context) (Connection, error)

// PoolConfig configures the connection pool
type PoolConfig struct {
	MaxSize         int           // Maximum pool size
	MinSize         int           // Minimum idle connections
	MaxIdleTime     time.Duration // Max time a connection can be idle
	MaxLifetime     time.Duration // Max lifetime of a connection
	AcquireTimeout  time.Duration // Timeout for acquiring a connection
	HealthCheckPeriod time.Duration // Period between health checks
}

// DefaultPoolConfig returns sensible defaults
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxSize:           100,
		MinSize:           10,
		MaxIdleTime:       5 * time.Minute,
		MaxLifetime:       30 * time.Minute,
		AcquireTimeout:    5 * time.Second,
		HealthCheckPeriod: 30 * time.Second,
	}
}

// pooledConn wraps a connection with metadata
type pooledConn struct {
	conn      Connection
	createdAt time.Time
	lastUsed  time.Time
}

// Pool manages a pool of connections for stateless operations
type Pool struct {
	config   PoolConfig
	factory  ConnectionFactory

	mu       sync.Mutex
	conns    []*pooledConn
	waiting  []chan *pooledConn

	size     int32  // Current number of connections
	closed   int32

	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewPool creates a new connection pool
func NewPool(config PoolConfig, factory ConnectionFactory) (*Pool, error) {
	if config.MaxSize <= 0 {
		config.MaxSize = 100
	}
	if config.MinSize < 0 {
		config.MinSize = 0
	}
	if config.MinSize > config.MaxSize {
		config.MinSize = config.MaxSize
	}

	p := &Pool{
		config:  config,
		factory: factory,
		conns:   make([]*pooledConn, 0, config.MaxSize),
		waiting: make([]chan *pooledConn, 0),
		stopCh:  make(chan struct{}),
	}

	// Pre-create minimum connections
	ctx := context.Background()
	for i := 0; i < config.MinSize; i++ {
		conn, err := factory(ctx)
		if err != nil {
			// Close already created connections
			p.Close()
			return nil, err
		}
		p.conns = append(p.conns, &pooledConn{
			conn:      conn,
			createdAt: time.Now(),
			lastUsed:  time.Now(),
		})
		atomic.AddInt32(&p.size, 1)
	}

	// Start health checker
	if config.HealthCheckPeriod > 0 {
		p.wg.Add(1)
		go p.healthChecker()
	}

	return p, nil
}

// Acquire gets a connection from the pool
func (p *Pool) Acquire(ctx context.Context) (Connection, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, ErrPoolClosed
	}

	// Try to get an existing connection
	p.mu.Lock()
	for len(p.conns) > 0 {
		// Pop from end (LIFO for better cache locality)
		pc := p.conns[len(p.conns)-1]
		p.conns = p.conns[:len(p.conns)-1]

		// Check if connection is still valid
		if p.isValidConnection(pc) {
			pc.lastUsed = time.Now()
			p.mu.Unlock()
			return pc.conn, nil
		}

		// Invalid connection, close it
		pc.conn.Close()
		atomic.AddInt32(&p.size, -1)
	}

	// No available connections, try to create new one
	currentSize := atomic.LoadInt32(&p.size)
	if int(currentSize) < p.config.MaxSize {
		atomic.AddInt32(&p.size, 1)
		p.mu.Unlock()

		conn, err := p.factory(ctx)
		if err != nil {
			atomic.AddInt32(&p.size, -1)
			return nil, err
		}
		return conn, nil
	}

	// Pool is at capacity, wait for a connection
	waitCh := make(chan *pooledConn, 1)
	p.waiting = append(p.waiting, waitCh)
	p.mu.Unlock()

	// Set up timeout
	var timeoutCh <-chan time.Time
	if p.config.AcquireTimeout > 0 {
		timer := time.NewTimer(p.config.AcquireTimeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case pc := <-waitCh:
		if pc == nil {
			return nil, ErrPoolClosed
		}
		pc.lastUsed = time.Now()
		return pc.conn, nil
	case <-timeoutCh:
		// Remove from waiting list
		p.mu.Lock()
		for i, ch := range p.waiting {
			if ch == waitCh {
				p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
				break
			}
		}
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	case <-ctx.Done():
		p.mu.Lock()
		for i, ch := range p.waiting {
			if ch == waitCh {
				p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
				break
			}
		}
		p.mu.Unlock()
		return nil, ctx.Err()
	case <-p.stopCh:
		return nil, ErrPoolClosed
	}
}

// Release returns a connection to the pool
func (p *Pool) Release(conn Connection) {
	if atomic.LoadInt32(&p.closed) == 1 {
		conn.Close()
		return
	}

	// Reset connection state for reuse
	if err := conn.Reset(); err != nil {
		conn.Close()
		atomic.AddInt32(&p.size, -1)
		return
	}

	pc := &pooledConn{
		conn:      conn,
		createdAt: time.Now(), // Approximate, but good enough
		lastUsed:  time.Now(),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// If someone is waiting, give them the connection
	if len(p.waiting) > 0 {
		waitCh := p.waiting[0]
		p.waiting = p.waiting[1:]
		waitCh <- pc
		return
	}

	// Otherwise, add back to pool
	p.conns = append(p.conns, pc)
}

// Close closes the pool and all connections
func (p *Pool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	close(p.stopCh)

	p.mu.Lock()
	// Close all idle connections
	for _, pc := range p.conns {
		pc.conn.Close()
	}
	p.conns = nil

	// Notify waiting goroutines
	for _, ch := range p.waiting {
		close(ch)
	}
	p.waiting = nil
	p.mu.Unlock()

	p.wg.Wait()
	return nil
}

// Stats returns pool statistics
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PoolStats{
		Size:        int(atomic.LoadInt32(&p.size)),
		Idle:        len(p.conns),
		Waiting:     len(p.waiting),
		MaxSize:     p.config.MaxSize,
		Closed:      atomic.LoadInt32(&p.closed) == 1,
	}
}

// PoolStats contains pool statistics
type PoolStats struct {
	Size    int
	Idle    int
	Waiting int
	MaxSize int
	Closed  bool
}

func (p *Pool) isValidConnection(pc *pooledConn) bool {
	// Check lifetime
	if p.config.MaxLifetime > 0 && time.Since(pc.createdAt) > p.config.MaxLifetime {
		return false
	}

	// Check idle time
	if p.config.MaxIdleTime > 0 && time.Since(pc.lastUsed) > p.config.MaxIdleTime {
		return false
	}

	// Check connection health
	return pc.conn.IsValid()
}

func (p *Pool) healthChecker() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkHealth()
		case <-p.stopCh:
			return
		}
	}
}

func (p *Pool) checkHealth() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove invalid connections
	validConns := make([]*pooledConn, 0, len(p.conns))
	for _, pc := range p.conns {
		if p.isValidConnection(pc) {
			validConns = append(validConns, pc)
		} else {
			pc.conn.Close()
			atomic.AddInt32(&p.size, -1)
		}
	}
	p.conns = validConns

	// Ensure minimum connections
	currentSize := int(atomic.LoadInt32(&p.size))
	needed := p.config.MinSize - currentSize
	if needed > 0 {
		ctx := context.Background()
		for i := 0; i < needed; i++ {
			conn, err := p.factory(ctx)
			if err != nil {
				break
			}
			p.conns = append(p.conns, &pooledConn{
				conn:      conn,
				createdAt: time.Now(),
				lastUsed:  time.Now(),
			})
			atomic.AddInt32(&p.size, 1)
		}
	}
}
