package federation

import (
	"context"
)

// SourceType defines the type of remote source
type SourceType string

const (
	SourcePostgres SourceType = "POSTGRES"
	SourceMySQL    SourceType = "MYSQL"
	SourceREST     SourceType = "REST"
)

// SourceConfig holds connection details
type SourceConfig struct {
	Name string            `json:"name"`
	Type SourceType        `json:"type"`
	URL  string            `json:"url"`
	Auth map[string]string `json:"auth"`
}

// Source represents a connection to a remote data source
type Source interface {
	// Connect establishes the connection
	Connect(ctx context.Context) error
	// Close closes the connection
	Close() error
	// Query executes a query against the source
	Query(ctx context.Context, query string, args ...interface{}) (interface{}, error)
	// Introspect returns the schema of the source
	Introspect(ctx context.Context) (map[string]interface{}, error)
}

// Manager handles multiple sources
type Manager struct {
	sources map[string]Source
}

func NewManager() *Manager {
	return &Manager{
		sources: make(map[string]Source),
	}
}

func (m *Manager) Register(name string, source Source) {
	m.sources[name] = source
}

func (m *Manager) Get(name string) Source {
	return m.sources[name]
}
