package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/platform/graphql"
	"github.com/lumadb/cluster/pkg/query"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.uber.org/zap"
)

// MCPServer implements the Model Context Protocol
type MCPServer struct {
	node      *cluster.Node
	gqlEngine *graphql.GraphQLEngine
	server    *server.MCPServer
	logger    *zap.Logger
}

func NewMCPServer(node *cluster.Node, gqlEngine *graphql.GraphQLEngine, logger *zap.Logger) *MCPServer {
	s := server.NewMCPServer(
		"LumaDB",
		"1.0.0",
	)

	ms := &MCPServer{
		node:      node,
		gqlEngine: gqlEngine,
		server:    s,
		logger:    logger,
	}

	ms.registerTools()
	ms.registerResources()

	return ms
}

func (s *MCPServer) registerTools() {
	// Tool: query_luma
	s.server.AddTool(mcp.NewTool(
		"query_luma",
		mcp.WithDescription("Execute a LQL (Luma Query Language) query against the database"),
		mcp.WithString("query", mcp.Required(), mcp.Description("The SQL-like query to execute")),
	), s.handleQueryLayer)

	// Tool: query_graphql
	s.server.AddTool(mcp.NewTool(
		"query_graphql",
		mcp.WithDescription("Execute a GraphQL query against the database (Hasura-style)"),
		mcp.WithString("query", mcp.Required(), mcp.Description("The GraphQL query string")),
		mcp.WithString("variables", mcp.Description("Optional variables as JSON string")),
	), s.handleGraphQLQuery)

	// Tool: list_collections
	s.server.AddTool(mcp.NewTool(
		"list_collections",
		mcp.WithDescription("List all collections in the database"),
	), s.handleListCollections)

	// Tool: inspect_schema
	s.server.AddTool(mcp.NewTool(
		"inspect_schema",
		mcp.WithDescription("Get the schema of a collection"),
		mcp.WithString("collection", mcp.Required(), mcp.Description("The collection name")),
	), s.handleInspectSchema)
}

func (s *MCPServer) registerResources() {
	// Dynamic resource listing would go here.
	// For now, we rely on tools.
}

// handleGraphQLQuery executes a GraphQL query
func (s *MCPServer) handleGraphQLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query, err := request.RequireString("query")
	if err != nil {
		return mcp.NewToolResultError("query argument is required"), nil
	}

	variablesStr := request.GetString("variables", "") // Optional with default empty
	var variables map[string]interface{}

	if variablesStr != "" {
		if err := json.Unmarshal([]byte(variablesStr), &variables); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid variables JSON: %v", err)), nil
		}
	}

	result := s.gqlEngine.Execute(ctx, query, variables)
	if len(result.Errors) > 0 {
		return mcp.NewToolResultError(fmt.Sprintf("GraphQL errors: %v", result.Errors)), nil
	}

	// Helper to marshal result.Data to JSON string
	return mcp.NewToolResultText(fmt.Sprintf("%v", result.Data)), nil
}

func (s *MCPServer) handleQueryLayer(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	queryStr, err := request.RequireString("query")
	if err != nil {
		return mcp.NewToolResultError("query argument is required"), nil
	}

	stmt, err := query.Parse(queryStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("parse error: %v", err)), nil
	}

	// Initial implementation only supports Select
	if stmt.Select != nil {
		// Construct query object for Rust core
		queryMap := map[string]interface{}{}
		if stmt.Select.Where != nil {
			queryMap["filter"] = stmt.Select.Where
		}
		if stmt.Select.Limit != nil {
			queryMap["limit"] = *stmt.Select.Limit
		}

		// Execute query against the database
		results, err := s.node.GetDatabase().Query(stmt.Select.From, queryMap)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("database error: %v", err)), nil
		}

		return mcp.NewToolResultText(fmt.Sprintf("%v", results)), nil
	}

	return mcp.NewToolResultText("Query type not supported yet via MCP"), nil
}

func (s *MCPServer) handleListCollections(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// MVP: Mock response or query system table
	// In real impl: s.node.ListCollections()
	collections := []string{"users", "products", "orders", "system.events"}
	return mcp.NewToolResultText(fmt.Sprintf("%v", collections)), nil
}

func (s *MCPServer) handleInspectSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	collection, err := request.RequireString("collection")
	if err != nil {
		return mcp.NewToolResultError("collection argument is required"), nil
	}
	// MVP: Mock schema
	schema := map[string]string{
		"id":         "string",
		"created_at": "datetime",
		"updated_at": "datetime",
	}
	return mcp.NewToolResultText(fmt.Sprintf("Schema for %s: %v", collection, schema)), nil
}

// ServeStdio serves MCP over standard input/output
func (s *MCPServer) ServeStdio() error {
	return server.ServeStdio(s.server)
}
