package ai

import (
	"fmt"
	"time"

	"github.com/lumadb/cluster/pkg/core"
	"go.uber.org/zap"
)

// RAGService orchestrates Retrieval Augmented Generation
type RAGService struct {
	db       *core.Database
	aiClient *Client
	logger   *zap.Logger
}

// NewRAGService creates a new RAG service
func NewRAGService(db *core.Database, aiClient *Client, logger *zap.Logger) *RAGService {
	return &RAGService{
		db:       db,
		aiClient: aiClient,
		logger:   logger,
	}
}

// IngestResult contains the ID of the ingested document
type IngestResult struct {
	DocumentID string `json:"document_id"`
}

// QueryResult contains the answer and source documents
type QueryResult struct {
	Answer          string                   `json:"answer"`
	Sources         []map[string]interface{} `json:"sources"`
	ExecutionTimeMs int64                    `json:"execution_time_ms"`
}

// Ingest processes text, generates embedding, and stores it
func (s *RAGService) Ingest(collection, text string, metadata map[string]interface{}) (*IngestResult, error) {
	// 1. Get Embedding
	embedding, err := s.aiClient.GetEmbedding(text)
	if err != nil {
		return nil, fmt.Errorf("failed to get embedding: %w", err)
	}

	// 2. Prepare Document
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	// "text" field to store original content
	// using "_vector" for internal vector storage (handled by Rust Core)
	metadata["text"] = text
	metadata["_vector"] = embedding

	// 3. Insert into Database
	// doc will be serialized to JSON/Bincode by luma.go binding
	id, err := s.db.Insert(collection, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to insert document: %w", err)
	}

	return &IngestResult{
		DocumentID: id,
	}, nil
}

// Query performs a RAG query
func (s *RAGService) Query(collection, question string) (*QueryResult, error) {
	start := time.Now()

	// 1. Embed Query
	embedding, err := s.aiClient.GetEmbedding(question)
	if err != nil {
		return nil, fmt.Errorf("failed to get query embedding: %w", err)
	}

	// 2. Vector Search (core.Database.VectorSearch)
	// Searching for top 5 most relevant documents
	vectorResults, err := s.db.VectorSearch(embedding, 5)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	// 3. Construct Context
	// We need to fetch the full documents for the IDs returned by vector search
	// VectorSearch currently returns [{"id": "...", "score": ...}]
	var contextDocs []string
	var sources []map[string]interface{}

	for _, res := range vectorResults { // Assuming result is []map[string]interface{}
		id, ok := res["id"].(string)
		if !ok {
			continue
		}
		// Fetch full document
		doc, err := s.db.Get(collection, id)
		if err != nil {
			s.logger.Warn("Failed to fetch context doc", zap.String("id", id), zap.Error(err))
			continue
		}

		if doc == nil {
			continue
		}

		if text, ok := doc["text"].(string); ok {
			contextDocs = append(contextDocs, text)
			sources = append(sources, doc)
		}
	}

	context := ""
	for i, text := range contextDocs {
		context += fmt.Sprintf("Source %d:\n%s\n\n", i+1, text)
	}

	// 4. Generate Answer (aiClient.Generate)
	// Using the Generate method which supports optional context
	genResp, err := s.aiClient.Generate(question, context, 500) // max 500 tokens
	if err != nil {
		return nil, fmt.Errorf("llm generation failed: %w", err)
	}

	return &QueryResult{
		Answer:          genResp.Response,
		Sources:         sources,
		ExecutionTimeMs: time.Since(start).Milliseconds(),
	}, nil
}
