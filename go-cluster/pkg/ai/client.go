package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client handles communication with the Python AI Service
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// Config for AI Client
type Config struct {
	Host string
	Port int
}

// NewClient creates a new AI service client
func NewClient(config Config) *Client {
	baseURL := fmt.Sprintf("http://%s:%d", config.Host, config.Port)
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GenerateRequest matches the Python API schema
type GenerateRequest struct {
	Prompt    string `json:"prompt"`
	Context   string `json:"context,omitempty"`
	MaxTokens int    `json:"max_tokens"`
}

// GenerateResponse matches the Python API schema
type GenerateResponse struct {
	Response string `json:"response"`
}

// Generate generates text using the AI service
func (c *Client) Generate(prompt string, context string, maxTokens int) (*GenerateResponse, error) {
	reqBody := GenerateRequest{
		Prompt:    prompt,
		Context:   context,
		MaxTokens: maxTokens,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(
		fmt.Sprintf("%s/generate", c.baseURL),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to AI service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ai service returned status: %d", resp.StatusCode)
	}

	var genResp GenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&genResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &genResp, nil
}

// EmbeddingRequest for getting vector embeddings
type EmbeddingRequest struct {
	Text string `json:"text"`
}

// EmbeddingResponse matches the Python API schema
type EmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

// GetEmbedding gets the vector embedding for a text
func (c *Client) GetEmbedding(text string) ([]float32, error) {
	reqBody := EmbeddingRequest{
		Text: text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Python AI service endpoint likely /embedding based on typical patterns,
	// but I need to check main.py.
	// main.py didn't show /embedding in the viewer earlier (only /generate and /health).
	// I need to add /embedding endpoint to Python service too!

	resp, err := c.httpClient.Post(
		fmt.Sprintf("%s/embedding", c.baseURL),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to AI service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ai service returned status: %d", resp.StatusCode)
	}

	var embResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return embResp.Embedding, nil
}

// HealthCheck checks if AI service is up
func (c *Client) HealthCheck() bool {
	resp, err := c.httpClient.Get(fmt.Sprintf("%s/health", c.baseURL))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
