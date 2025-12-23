package cluster

import (
	"bytes"
	"fmt"
	"sync"
)

// Region represents a range of keys managed by a Raft Group
type Region struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte // Exclusive
	Epoch    RegionEpoch
	Peers    []Peer
}

type RegionEpoch struct {
	ConfVer uint64
	Version uint64
}

type Peer struct {
	ID      uint64
	StoreID uint64
}

// RegionManager handles region splitting, merging, and routing
type RegionManager struct {
	mu      sync.RWMutex
	regions map[uint64]*Region
	store   StorageEngine // To persist region meta
}

func NewRegionManager(store StorageEngine) *RegionManager {
	return &RegionManager{
		regions: make(map[uint64]*Region),
		store:   store,
	}
}

// GetRegionByKey finds the region containing the key
func (rm *RegionManager) GetRegionByKey(key []byte) (*Region, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, r := range rm.regions {
		if rm.keyInRegion(key, r) {
			return r, nil
		}
	}
	return nil, fmt.Errorf("region not found for key")
}

func (rm *RegionManager) keyInRegion(key []byte, r *Region) bool {
	return bytes.Compare(key, r.StartKey) >= 0 &&
		(len(r.EndKey) == 0 || bytes.Compare(key, r.EndKey) < 0)
}

// SplitRegion splits a region into two at the splitKey
func (rm *RegionManager) SplitRegion(regionID uint64, splitKey []byte) (*Region, *Region, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	original, ok := rm.regions[regionID]
	if !ok {
		return nil, nil, fmt.Errorf("region not found")
	}

	// Validation
	if !rm.keyInRegion(splitKey, original) {
		return nil, nil, fmt.Errorf("split key out of bounds")
	}

	// Create new region (Right)
	newRegionID := regionID + 1000 // Simplified ID generation
	rightRegion := &Region{
		ID:       newRegionID,
		StartKey: splitKey,
		EndKey:   original.EndKey,
		Epoch:    RegionEpoch{ConfVer: 1, Version: 1},
		Peers:    original.Peers,
	}

	// Update original (Left)
	original.EndKey = splitKey
	original.Epoch.Version++

	// Persist
	rm.regions[newRegionID] = rightRegion

	return original, rightRegion, nil
}
