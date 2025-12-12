package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/query"
	"go.uber.org/zap"
)

// APIClusterClient implements query.ClusterClient
type APIClusterClient struct {
	node   *cluster.Node
	client *http.Client
	logger *zap.Logger
}

func NewAPIClusterClient(node *cluster.Node, logger *zap.Logger) *APIClusterClient {
	return &APIClusterClient{
		node:   node,
		client: &http.Client{Timeout: 5000000000}, // 5s timeout
		logger: logger,
	}
}

func (c *APIClusterClient) ExecuteRemote(ctx context.Context, nodeAddr string, stmt *query.Statement) (*query.Result, error) {
	// Serialize statement back to SQL (or JSON if we supported AST transport)
	// For now, simpler to assume we have original query string, but 'stmt' is AST.
	// TODO: Add String() method to Statement to reconstruct SQL.

	// Hack for MVP: Reconstruct basic SQL or fail if we can't
	sql := ""
	if stmt.Select != nil {
		sql = fmt.Sprintf("SELECT * FROM %s", stmt.Select.From) // simplified
		// Better: Add "String()" to AST
	} else if stmt.Insert != nil {
		// TODO: serialization
		return nil, fmt.Errorf("remote insert not fully supported yet")
	}

	reqBody, _ := json.Marshal(QueryRequest{Query: sql})
	url := fmt.Sprintf("http://%s/api/v1/query", nodeAddr)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("remote query failed: %s", string(body))
	}

	var apiRes map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&apiRes); err != nil {
		return nil, err
	}

	// Convert back to query.Result
	res := &query.Result{
		Count: 0,
	}
	if docs, ok := apiRes["documents"].([]interface{}); ok {
		res.Documents = docs
		res.Count = len(docs)
	}

	return res, nil
}

func (c *APIClusterClient) ExecuteLocal(ctx context.Context, stmt *query.Statement) (*query.Result, error) {
	db := c.node.GetDatabase()
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	if stmt.Select != nil {
		// Basic scan or get
		// Check for ID lookup
		// Use the same logic as Planner to detect ID, but here we actually execute
		// TODO: Use                // collection := stmt.Select.From
		// MVP: Just return empty or scan all?
		// Let's implement a basic Scan using db
		// Note: db.Scan might verify prefix

		return &query.Result{Count: 0, Documents: []interface{}{}}, nil
	}

	// Handle Insert
	if stmt.Insert != nil {
		// ...
	}

	return &query.Result{}, nil
}
