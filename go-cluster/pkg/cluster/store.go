package cluster

import (
	"errors"
)

// StorageEngine defines the interface for persistent Raft storage (WAL + State)
// This abstracts over BadgerDB, RocksDB, or LumaDB's own engine.
type StorageEngine interface {
	// Log operations
	InitialState() (RaftState, error)
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	Snapshot() (Snapshot, error)

	// Write operations
	Save(st RaftState, ents []Entry, snap Snapshot) error
}

// RaftState encapsulates HardState and ConfState
type RaftState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// Entry is a Raft log entry
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType // Normal, ConfChange
	Data  []byte
}

type EntryType int

const (
	EntryNormal     EntryType = 0
	EntryConfChange EntryType = 1
)

// Snapshot represents a point-in-time state
type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

type SnapshotMetadata struct {
	Index uint64
	Term  uint64
	// ConfState ...
}

var ErrCompacted = errors.New("requested index is compacted")
var ErrSnapOutOfDate = errors.New("snapshot out of date")
var ErrUnavailable = errors.New("requested index unavailable")
