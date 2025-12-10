// Package api implements HTTP and gRPC APIs for the cluster
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tdb-plus/cluster/pkg/cluster"
	"github.com/tdb-plus/cluster/pkg/router"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the HTTP API server
type Server struct {
	node   *cluster.Node
	router *router.Router
	logger *zap.Logger
	engine *gin.Engine
}

// NewServer creates a new API server
func NewServer(node *cluster.Node, rtr *router.Router, logger *zap.Logger) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &Server{
		node:   node,
		router: rtr,
		logger: logger,
		engine: engine,
	}

	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Health check
	s.engine.GET("/health", s.handleHealth)

	// Cluster info
	s.engine.GET("/cluster", s.handleClusterInfo)
	s.engine.GET("/cluster/topology", s.handleTopology)

	// Query API (stateless operations)
	api := s.engine.Group("/api/v1")
	{
		// Document operations
		api.POST("/query", s.handleQuery)
		api.GET("/collections/:collection/:id", s.handleGet)
		api.POST("/collections/:collection", s.handleInsert)
		api.PUT("/collections/:collection/:id", s.handleUpdate)
		api.DELETE("/collections/:collection/:id", s.handleDelete)

		// Batch operations
		api.POST("/batch", s.handleBatch)

		// Collection management
		api.GET("/collections", s.handleListCollections)
		api.POST("/collections/:collection/indexes", s.handleCreateIndex)
	}

	// Metrics
	s.engine.GET("/metrics", s.handleMetrics)
}

// Handler returns the HTTP handler
func (s *Server) Handler() http.Handler {
	return s.engine
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"node_id":   s.node.IsLeader(),
		"is_leader": s.node.IsLeader(),
		"timestamp": time.Now().Unix(),
	})
}

func (s *Server) handleClusterInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"is_leader":   s.node.IsLeader(),
		"leader_addr": s.node.LeaderAddr(),
		"peers":       s.node.GetPeers(),
	})
}

func (s *Server) handleTopology(c *gin.Context) {
	c.JSON(http.StatusOK, s.router.GetClusterTopology())
}

func (s *Server) handleQuery(c *gin.Context) {
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Route the query to appropriate node
	target, err := s.router.Route(c.Request.Context(), req.Collection, []byte(req.Query))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// If local, execute; otherwise forward
	if target == "localhost" || s.node.IsLeader() {
		// Execute locally
		// TODO: Integrate with Rust storage engine
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"documents": []interface{}{},
			"count":     0,
		})
	} else {
		// Forward to leader
		c.JSON(http.StatusTemporaryRedirect, gin.H{
			"redirect": target,
		})
	}
}

func (s *Server) handleGet(c *gin.Context) {
	collection := c.Param("collection")
	id := c.Param("id")

	// Route read request
	_, err := s.router.RouteRead(c.Request.Context(), collection, []byte(id))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// TODO: Integrate with Rust storage engine
	c.JSON(http.StatusOK, gin.H{
		"_id":        id,
		"collection": collection,
	})
}

func (s *Server) handleInsert(c *gin.Context) {
	collection := c.Param("collection")

	var doc map[string]interface{}
	if err := c.ShouldBindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Must go through Raft for consistency
	if !s.node.IsLeader() {
		c.JSON(http.StatusTemporaryRedirect, gin.H{
			"redirect": s.node.LeaderAddr(),
		})
		return
	}

	// Apply via Raft
	docBytes, _ := json.Marshal(doc)
	cmd := &cluster.Command{
		Op:         "set",
		Collection: collection,
		Key:        doc["_id"].(string),
		Value:      docBytes,
	}

	if err := s.node.Apply(cmd, 5*time.Second); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status": "created",
		"_id":    doc["_id"],
	})
}

func (s *Server) handleUpdate(c *gin.Context) {
	collection := c.Param("collection")
	id := c.Param("id")

	var doc map[string]interface{}
	if err := c.ShouldBindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	doc["_id"] = id

	if !s.node.IsLeader() {
		c.JSON(http.StatusTemporaryRedirect, gin.H{
			"redirect": s.node.LeaderAddr(),
		})
		return
	}

	docBytes, _ := json.Marshal(doc)
	cmd := &cluster.Command{
		Op:         "set",
		Collection: collection,
		Key:        id,
		Value:      docBytes,
	}

	if err := s.node.Apply(cmd, 5*time.Second); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "updated",
		"_id":    id,
	})
}

func (s *Server) handleDelete(c *gin.Context) {
	collection := c.Param("collection")
	id := c.Param("id")

	if !s.node.IsLeader() {
		c.JSON(http.StatusTemporaryRedirect, gin.H{
			"redirect": s.node.LeaderAddr(),
		})
		return
	}

	cmd := &cluster.Command{
		Op:         "delete",
		Collection: collection,
		Key:        id,
	}

	if err := s.node.Apply(cmd, 5*time.Second); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "deleted",
		"_id":    id,
	})
}

func (s *Server) handleBatch(c *gin.Context) {
	var req BatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Process batch operations
	results := make([]map[string]interface{}, 0, len(req.Operations))
	for _, op := range req.Operations {
		results = append(results, map[string]interface{}{
			"op":     op.Op,
			"status": "ok",
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"results": results,
	})
}

func (s *Server) handleListCollections(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"collections": []string{},
	})
}

func (s *Server) handleCreateIndex(c *gin.Context) {
	collection := c.Param("collection")

	var req CreateIndexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":     "created",
		"collection": collection,
		"index":      req.Name,
	})
}

func (s *Server) handleMetrics(c *gin.Context) {
	// TODO: Prometheus metrics
	c.String(http.StatusOK, "# TDB+ Metrics\n")
}

// Request/Response types
type QueryRequest struct {
	Query      string `json:"query"`
	Language   string `json:"language"`
	Collection string `json:"collection,omitempty"`
}

type BatchRequest struct {
	Operations []BatchOperation `json:"operations"`
}

type BatchOperation struct {
	Op         string                 `json:"op"`
	Collection string                 `json:"collection"`
	Document   map[string]interface{} `json:"document,omitempty"`
	ID         string                 `json:"id,omitempty"`
}

type CreateIndexRequest struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Type   string   `json:"type"`
	Unique bool     `json:"unique"`
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(node *cluster.Node, rtr *router.Router, logger *zap.Logger) *grpc.Server {
	server := grpc.NewServer()
	// TODO: Register gRPC services
	return server
}
