package platform

import (
	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/platform/auth"
	"github.com/lumadb/cluster/pkg/platform/graphql"
	"github.com/lumadb/cluster/pkg/platform/mcp"
	"go.uber.org/zap"
)

// Platform manages the high-level application features (GraphQL, Events, Auth)
type Platform struct {
	node       *cluster.Node
	logger     *zap.Logger
	mcpServer  *mcp.MCPServer
	gqlEngine  *graphql.GraphQLEngine
	authEngine *auth.AuthEngine
}

func NewPlatform(node *cluster.Node, logger *zap.Logger) *Platform {
	return &Platform{
		node:   node,
		logger: logger,
	}
}

// Start initializes all platform subsystems
func (p *Platform) Start() error {
	p.logger.Info("Starting Luma Platform...")

	// 1. Start GraphQL Engine (needed by MCP)
	p.gqlEngine = graphql.NewGraphQLEngine(p.node, p.logger)
	if err := p.gqlEngine.BuildSchema(); err != nil {
		p.logger.Error("Failed to build GraphQL schema", zap.Error(err))
	}

	// 2. Start MCP Server
	p.mcpServer = mcp.NewMCPServer(p.node, p.gqlEngine, p.logger)

	// 3. Start Auth Engine
	p.authEngine = auth.NewAuthEngine(p.node, p.logger)
	if err := p.authEngine.Start(); err != nil {
		p.logger.Error("Failed to start Auth Engine", zap.Error(err))
	}

	return nil
}
