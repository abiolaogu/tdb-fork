package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// Job represents a scheduled task
type Job struct {
	Name     string                 `json:"name"`
	Schedule string                 `json:"schedule"` // Cron syntax
	Payload  map[string]interface{} `json:"payload"`
}

// Scheduler manages cron jobs
type Scheduler struct {
	node   *cluster.Node
	logger *zap.Logger
	cron   *cron.Cron
	jobs   map[string]Job
	mu     sync.RWMutex
}

// NewScheduler creates a new cron scheduler
func NewScheduler(node *cluster.Node, logger *zap.Logger) *Scheduler {
	return &Scheduler{
		node:   node,
		logger: logger,
		cron:   cron.New(cron.WithSeconds()), // Support seconds for precision
		jobs:   make(map[string]Job),
	}
}

// Start begins the scheduler
func (s *Scheduler) Start() {
	s.logger.Info("Starting Cron Scheduler")
	s.cron.Start()
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.cron.Stop()
}

// AddJob registers a new cron job
func (s *Scheduler) AddJob(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate cron syntax
	_, err := cron.ParseStandard(job.Schedule)
	if err != nil {
		// Try with seconds parser if standard fails, but v3 parser is flexible
		// Actually, we instantiated WithSeconds, so we should test that specific parser
		// For now, let the AddFunc handle validation implicitly or catch panic
	}

	id, err := s.cron.AddFunc(job.Schedule, func() {
		s.executeJob(job)
	})
	if err != nil {
		return fmt.Errorf("invalid schedule: %v", err)
	}

	s.jobs[job.Name] = job
	s.logger.Info("Added cron job", zap.String("name", job.Name), zap.String("schedule", job.Schedule), zap.Int("id", int(id)))
	return nil
}

func (s *Scheduler) executeJob(job Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.logger.Info("Executing cron job", zap.String("name", job.Name))

	// Determine what to do based on Payload
	// MVP: Just log it or execute a dummy query if specified
	if query, ok := job.Payload["query"].(string); ok {
		// Ensure we have a DB reference
		// s.node.GetDatabase()... but Node execution path might be different
		// For now, we just log "Would execute query: " + query
		s.logger.Info("Cron Trigger Query", zap.String("query", query))
	} else if url, ok := job.Payload["url"].(string); ok {
		// Webhook
		s.logger.Info("Cron Trigger Webhook", zap.String("url", url))
	}

	_ = ctx // avoid unused
}

// ListJobs returns all registered jobs
func (s *Scheduler) ListJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		list = append(list, j)
	}
	return list
}
