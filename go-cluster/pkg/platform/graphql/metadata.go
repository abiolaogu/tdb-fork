package graphql

import (
	"fmt"
	"sync"
)

// MetadataStore manages schema configuration, relationships, and permissions
type MetadataStore struct {
	mu            sync.RWMutex
	Tables        map[string]*TableMetadata `json:"tables"`
	Relationships map[string]*Relationship  `json:"relationships"` // key: "table.name"
}

type TableMetadata struct {
	Name        string                 `json:"name"`
	Permissions map[string]*RoleConfig `json:"permissions"` // key: role name
}

type RoleConfig struct {
	Role   string            `json:"role"`
	Select *PermissionConfig `json:"select"`
	Insert *PermissionConfig `json:"insert"`
	Update *PermissionConfig `json:"update"`
	Delete *PermissionConfig `json:"delete"`
}

type PermissionConfig struct {
	Filter  map[string]interface{} `json:"filter"`  // Hasura-style boolean filter
	Columns []string               `json:"columns"` // Allowed columns
	Limit   int                    `json:"limit"`
}

type Relationship struct {
	Name         string            `json:"name"`
	FromTable    string            `json:"from_table"`
	ToTable      string            `json:"to_table"`
	Type         string            `json:"type"`          // object (1:1) or array (1:N)
	FieldMapping map[string]string `json:"field_mapping"` // "foreign_key": "private_key"
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		Tables:        make(map[string]*TableMetadata),
		Relationships: make(map[string]*Relationship),
	}
}

// Save persists the metadata to the internal storage (mocked for MVP)
func (ms *MetadataStore) Save() error {
	// In real impl: write to _schema_metadata collection
	return nil
}

// Load loads metadata from storage
func (ms *MetadataStore) Load() error {
	// In real impl: read from _schema_metadata collection
	return nil
}

func (ms *MetadataStore) TrackTable(tableName string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exists := ms.Tables[tableName]; !exists {
		ms.Tables[tableName] = &TableMetadata{
			Name:        tableName,
			Permissions: make(map[string]*RoleConfig),
		}
	}
}

func (ms *MetadataStore) AddRelationship(rel *Relationship) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	key := fmt.Sprintf("%s.%s", rel.FromTable, rel.Name)
	ms.Relationships[key] = rel
}

func (ms *MetadataStore) GetRelationships(tableName string) []*Relationship {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var rels []*Relationship
	for _, r := range ms.Relationships {
		if r.FromTable == tableName {
			rels = append(rels, r)
		}
	}
	return rels
}
