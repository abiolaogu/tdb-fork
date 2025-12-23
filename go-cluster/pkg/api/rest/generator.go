package rest

import (
	"net/http"
	"strconv"

	"tdb-fork/go-cluster/pkg/platform/graphql" // Assuming MetadataStore is used here or similar

	"github.com/gin-gonic/gin"
)

// Generator automatically creates REST endpoints for collections
type Generator struct {
	router   *gin.Engine
	metadata *graphql.MetadataStore
	// In a real implementation, we'd need access to the data layer (e.g., LumaDB Go client)
}

func NewGenerator(router *gin.Engine, metadata *graphql.MetadataStore) *Generator {
	return &Generator{
		router:   router,
		metadata: metadata,
	}
}

// RegisterRoutes generates CRUD endpoints for all tables
func (g *Generator) RegisterRoutes() {
	tables := g.metadata.GetTables()

	api := g.router.Group("/api/v1")

	for _, table := range tables {
		tableName := table.Name

		// LIST /api/v1/:collection
		api.GET("/"+tableName, g.handleList(tableName))

		// GET /api/v1/:collection/:id
		api.GET("/"+tableName+"/:id", g.handleGet(tableName))

		// POST /api/v1/:collection
		api.POST("/"+tableName, g.handleCreate(tableName))

		// PUT /api/v1/:collection/:id
		api.PUT("/"+tableName+"/:id", g.handleUpdate(tableName))

		// DELETE /api/v1/:collection/:id
		api.DELETE("/"+tableName+"/:id", g.handleDelete(tableName))
	}
}

func (g *Generator) handleList(collection string) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
		offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
		// where := c.Query("where")
		// orderBy := c.Query("order_by")

		// Mock response for now
		c.JSON(http.StatusOK, gin.H{
			"collection": collection,
			"limit":      limit,
			"offset":     offset,
			"data":       []gin.H{{"id": 1, "name": "Mock Data 1"}, {"id": 2, "name": "Mock Data 2"}},
			"total":      2,
		})
	}
}

func (g *Generator) handleGet(collection string) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		c.JSON(http.StatusOK, gin.H{
			"collection": collection,
			"id":         id,
			"data":       gin.H{"id": id, "name": "Single Mock Data"},
		})
	}
}

func (g *Generator) handleCreate(collection string) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"collection": collection,
			"status":     "created",
			"data":       payload,
		})
	}
}

func (g *Generator) handleUpdate(collection string) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		var payload interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"collection": collection,
			"id":         id,
			"status":     "updated",
			"data":       payload,
		})
	}
}

func (g *Generator) handleDelete(collection string) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		c.JSON(http.StatusOK, gin.H{
			"collection": collection,
			"id":         id,
			"status":     "deleted",
		})
	}
}
