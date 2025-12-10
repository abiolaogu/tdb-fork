// TDB+ Cluster Server
// Distributed coordination layer with Raft consensus
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lumadb/cluster/pkg/api"
	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/config"
	"github.com/lumadb/cluster/pkg/router"
	"go.uber.org/zap"
)

func main() {
	// Parse flags
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Node ID")
	httpAddr := flag.String("http-addr", ":8080", "HTTP API address")
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC address")
	raftAddr := flag.String("raft-addr", ":10000", "Raft address")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	join := flag.String("join", "", "Existing cluster node to join")
	flag.Parse()

	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load configuration
	cfg := config.DefaultConfig()
	if *configPath != "" {
		var err error
		cfg, err = config.LoadConfig(*configPath)
		if err != nil {
			logger.Fatal("Failed to load config", zap.Error(err))
		}
	}

	// Override with command line flags
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = fmt.Sprintf("node-%s-%d", hostname, time.Now().Unix())
	}
	cfg.HTTPAddr = *httpAddr
	cfg.GRPCAddr = *grpcAddr
	cfg.RaftAddr = *raftAddr
	cfg.DataDir = *dataDir

	logger.Info("Starting TDB+ Cluster Node",
		zap.String("node_id", cfg.NodeID),
		zap.String("http_addr", cfg.HTTPAddr),
		zap.String("grpc_addr", cfg.GRPCAddr),
		zap.String("raft_addr", cfg.RaftAddr),
	)

	// Create cluster node
	node, err := cluster.NewNode(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to create cluster node", zap.Error(err))
	}

	// Start the node
	if *join != "" {
		if err := node.Join(*join); err != nil {
			logger.Fatal("Failed to join cluster", zap.Error(err))
		}
	} else {
		if err := node.Bootstrap(); err != nil {
			logger.Fatal("Failed to bootstrap cluster", zap.Error(err))
		}
	}

	// Create router for request distribution
	rtr := router.NewRouter(node, logger)

	// Create HTTP API server
	apiServer := api.NewServer(node, rtr, logger)

	// Start HTTP server
	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: apiServer.Handler(),
	}

	go func() {
		logger.Info("HTTP server starting", zap.String("addr", cfg.HTTPAddr))
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("Failed to listen for gRPC", zap.Error(err))
	}

	grpcServer := api.NewGRPCServer(node, rtr, logger)
	go func() {
		logger.Info("gRPC server starting", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	httpServer.Shutdown(ctx)
	grpcServer.GracefulStop()
	node.Shutdown()

	logger.Info("Shutdown complete")
}
