package cluster

import (
	"os"
	"testing"
	"time"

	"github.com/lumadb/cluster/pkg/config"
	"go.uber.org/zap"
)

func TestNewNode(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "lumadb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Config
	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.NodeID = "node1"
	cfg.RaftAddr = "127.0.0.1:0" // Random port

	// Logger
	logger := zap.NewNop()

	// Create node
	node, err := NewNode(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()

	if node == nil {
		t.Fatal("Node is nil")
	}

	// Bootstrap
	err = node.Bootstrap()
	if err != nil {
		t.Fatalf("Failed to bootstrap: %v", err)
	}

	// Wait for leader
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	isLeader := false
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for leader")
		case <-ticker.C:
			if node.IsLeader() {
				isLeader = true
				goto Done
			}
		}
	}
Done:

	if !isLeader {
		t.Fatal("Node did not become leader")
	}
}
