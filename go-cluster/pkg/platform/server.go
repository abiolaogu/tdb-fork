package platform

import (
	"net/http"

	"strings"

	"github.com/gin-gonic/gin"
	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/platform/auth"
	"github.com/lumadb/cluster/pkg/platform/cron"
	gql "github.com/lumadb/cluster/pkg/platform/graphql"
	"go.uber.org/zap"
)

// Server serves REST and GraphQL APIs
type Server struct {
	node       *cluster.Node
	logger     *zap.Logger
	gqlEngine  *gql.GraphQLEngine
	authEngine *auth.AuthEngine
	cron       *cron.Scheduler
	router     *gin.Engine
}

func NewServer(node *cluster.Node, logger *zap.Logger) *Server {
	return &Server{
		node:       node,
		logger:     logger,
		gqlEngine:  gql.NewGraphQLEngine(node, logger),
		authEngine: auth.NewAuthEngine(node, logger),
		cron:       cron.NewScheduler(node, logger),
		router:     gin.Default(),
	}
}

func (s *Server) Start(addr string) error {
	s.logger.Info("Starting LumaDB Platform Server", zap.String("addr", addr))

	// Start Cron
	s.cron.Start()
	defer s.cron.Stop()

	// Initialize Schema
	if err := s.gqlEngine.BuildSchema(); err != nil {
		s.logger.Error("Failed to build GraphQL schema", zap.Error(err))
		// Continue anyway, might rebuild later
	}

	// Middleware
	s.router.Use(corsMiddleware())

	// Public Auth Routes
	s.router.POST("/api/auth/login", s.handleLogin)

	// Protected Routes Group
	protected := s.router.Group("/", s.authMiddleware())

	// GraphQL Endpoint (Protected)
	protected.POST("/graphql", s.handleGraphQL)
	protected.GET("/graphql", s.handleGraphQLOrPlayground)

	// REST API - Auto-generated routes
	api := s.router.Group("/api")
	{
		api.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "version": "2.0.0"})
		})

		// LumaDB Platform APIs (Protected)
		v1 := api.Group("/v1", s.authMiddleware())
		{
			// Stats
			v1.GET("/stats", s.handleStats)
			// Dynamic REST endpoints
			// GET /api/v1/:collection -> List
			// POST /api/v1/:collection -> Insert
			// GET /api/v1/:collection/:id -> Get
			v1.GET("/:collection", s.handleRestList)
			v1.POST("/:collection", s.handleRestInsert)
			v1.GET("/:collection/:id", s.handleRestGet)
		}
	}

	return s.router.Run(addr)
}

func (s *Server) handleGraphQL(c *gin.Context) {
	var body struct {
		Query     string                 `json:"query"`
		Operation string                 `json:"operationName"`
		Variables map[string]interface{} `json:"variables"`
	}

	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	result := s.gqlEngine.Execute(c.Request.Context(), body.Query, body.Variables)
	c.JSON(http.StatusOK, result)
}

func (s *Server) handleGraphQLOrPlayground(c *gin.Context) {
	// If query params present, execute
	query := c.Query("query")
	if query != "" {
		result := s.gqlEngine.Execute(c.Request.Context(), query, nil)
		c.JSON(http.StatusOK, result)
		return
	}

	// Otherwise serve GraphiQL
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, graphiqlHTML)
}

// Simple REST Handlers
func (s *Server) handleRestList(c *gin.Context) {
	collection := c.Param("collection")
	role := c.GetString("role")
	if !s.authEngine.IsAuthorized(role, auth.ActionRead) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
		return
	}
	// TODO: Parse limit/offset/filter from query params
	// Delegate to Node/DB
	// docs, err := s.node.RunQuery(collection, map[string]interface{}{"limit": 100})
	// Mock response for now
	c.JSON(http.StatusOK, gin.H{"collection": collection, "data": []interface{}{}})
}

func (s *Server) handleRestInsert(c *gin.Context) {
	collection := c.Param("collection")
	role := c.GetString("role")
	if !s.authEngine.IsAuthorized(role, auth.ActionWrite) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
		return
	}

	var doc map[string]interface{}
	if err := c.BindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// id, err := s.node.InsertDocument(collection, doc)
	c.JSON(http.StatusCreated, gin.H{"collection": collection, "status": "inserted"})
}

// handleStats returns system statistics
func (s *Server) handleStats(c *gin.Context) {
	// In a real system, we'd query the Node/Database for these
	// s.node.GetStats()

	// Mock stats for now, demonstrating structure
	stats := map[string]interface{}{
		"collections":  12,
		"documents":    1250000,
		"ops_per_sec":  450,
		"latency_p99":  "0.8ms",
		"nodes_active": 3,
		"events_fired": 85200,
	}

	c.JSON(http.StatusOK, stats)
}

func (s *Server) handleRestGet(c *gin.Context) {
	collection := c.Param("collection")
	id := c.Param("id")
	role := c.GetString("role")
	if !s.authEngine.IsAuthorized(role, auth.ActionRead) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
		return
	}
	// doc, err := s.node.GetDocument(collection, id)
	c.JSON(http.StatusOK, gin.H{"collection": collection, "id": id, "data": nil})
}

func (s *Server) handleLogin(c *gin.Context) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := c.BindJSON(&creds); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// Mock User Validation (In real world, check DB)
	if creds.Username == "admin" && creds.Password == "password" {
		token, err := s.authEngine.GenerateToken("admin-user-id", "admin")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"token": token})
		return
	}

	c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header required"})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization format"})
			return
		}

		tokenString := parts[1]
		claims, err := s.authEngine.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			return
		}

		// Inject user info into context
		c.Set("user_id", claims.UserID)
		c.Set("role", claims.Role)
		c.Next()
	}
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

const graphiqlHTML = `
<!DOCTYPE html>
<html>
  <head>
    <title>LumaDB GraphiQL</title>
    <link href="https://unpkg.com/graphiql/graphiql.min.css" rel="stylesheet" />
  </head>
  <body style="margin: 0;">
    <div id="graphiql" style="height: 100vh;"></div>
    <script
      crossorigin
      src="https://unpkg.com/react/umd/react.production.min.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/react-dom/umd/react-dom.production.min.js"
    ></script>
    <script
      crossorigin
      src="https://unpkg.com/graphiql/graphiql.min.js"
    ></script>
    <script>
      const fetcher = GraphiQL.createFetcher({
        url: '/graphql',
      });
      ReactDOM.render(
        React.createElement(GraphiQL, { fetcher: fetcher }),
        document.getElementById('graphiql'),
      );
    </script>
  </body>
</html>
`
