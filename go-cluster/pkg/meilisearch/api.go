package meilisearch

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// NOTE: These structs would connect to Rust via FFI/RPC in production.
// For now, they are in-memory mocks.

type SearchEngine struct {
	// Connection to Rust core or internal implementation
}

func (se *SearchEngine) Search(index string, req *SearchRequest) (*SearchResponse, error) {
	// Mock search
	return &SearchResponse{
		Hits:               []map[string]interface{}{},
		EstimatedTotalHits: 0,
		ProcessingTimeMs:   1,
		Query:              req.Q,
	}, nil
}

func (se *SearchEngine) AddDocuments(index string, docs []map[string]interface{}, pk string) error {
	return nil
}

func (se *SearchEngine) GetSettings(index string) (*IndexSettings, error) {
	return &IndexSettings{}, nil
}

type TaskManager struct {
	mu     sync.Mutex
	tasks  map[int64]*Task
	nextID int64
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks:  make(map[int64]*Task),
		nextID: 1,
	}
}

func (tm *TaskManager) Enqueue(typ TaskType, index string, details map[string]interface{}) *Task {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	id := tm.nextID
	tm.nextID++

	t := &Task{
		UID:        id,
		IndexUID:   index,
		Status:     "enqueued",
		Type:       typ,
		Details:    details,
		EnqueuedAt: time.Now(),
	}
	tm.tasks[id] = t
	return t
}

func (tm *TaskManager) Fail(id int64, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tasks[id]; ok {
		t.Status = "failed"
		t.Error = &TaskError{Message: err.Error()}
	}
}

func (tm *TaskManager) Complete(id int64, details map[string]interface{}) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.tasks[id]; ok {
		t.Status = "succeeded"
		// Merge details if needed
	}
}

type KeyManager struct{}

func NewKeyManager() *KeyManager { return &KeyManager{} }

// API implements Meilisearch REST API
type API struct {
	engine *SearchEngine
	tasks  *TaskManager
	keys   *KeyManager
	mu     sync.RWMutex
}

func NewAPI(engine *SearchEngine) *API {
	return &API{
		engine: engine,
		tasks:  NewTaskManager(),
		keys:   NewKeyManager(),
	}
}

// Register all Meilisearch-compatible endpoints
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/indexes", a.handleIndexes)
	mux.HandleFunc("/indexes/", a.handleIndex)
	mux.HandleFunc("/multi-search", a.handleMultiSearch)
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/stats", a.handleStats)
	mux.HandleFunc("/version", a.handleVersion)
	mux.HandleFunc("/tasks", a.handleTasks)
	mux.HandleFunc("/tasks/", a.handleTask)
	mux.HandleFunc("/keys", a.handleKeys)
	mux.HandleFunc("/keys/", a.handleKey)
	mux.HandleFunc("/dumps", a.handleDumps)
	mux.HandleFunc("/experimental-features", a.handleExperimentalFeatures)
}

// Handlers Stubs
func (a *API) handleIndexes(w http.ResponseWriter, r *http.Request) {
	// List indexes or create
	a.respond(w, []string{}, http.StatusOK)
}
func (a *API) handleIndex(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/indexes/")
	parts := strings.SplitN(path, "/", 2)
	uid := parts[0]

	if len(parts) == 1 {
		// Index CRU
		a.respond(w, map[string]string{"uid": uid}, http.StatusOK)
		return
	}

	subPath := parts[1]

	if subPath == "search" {
		a.handleSearch(w, r, uid)
	} else if strings.HasPrefix(subPath, "documents") {
		a.handleDocuments(w, r, uid, strings.TrimPrefix(subPath, "documents"))
	} else if strings.HasPrefix(subPath, "settings") {
		a.handleSettings(w, r, uid, strings.TrimPrefix(subPath, "settings"))
	} else {
		http.NotFound(w, r)
	}
}

func (a *API) handleMultiSearch(w http.ResponseWriter, r *http.Request) {}
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.respond(w, map[string]string{"status": "available"}, 200)
}
func (a *API) handleStats(w http.ResponseWriter, r *http.Request)                {}
func (a *API) handleVersion(w http.ResponseWriter, r *http.Request)              {}
func (a *API) handleTasks(w http.ResponseWriter, r *http.Request)                {}
func (a *API) handleTask(w http.ResponseWriter, r *http.Request)                 {}
func (a *API) handleKeys(w http.ResponseWriter, r *http.Request)                 {}
func (a *API) handleKey(w http.ResponseWriter, r *http.Request)                  {}
func (a *API) handleDumps(w http.ResponseWriter, r *http.Request)                {}
func (a *API) handleExperimentalFeatures(w http.ResponseWriter, r *http.Request) {}

// Search endpoint
func (a *API) handleSearch(w http.ResponseWriter, r *http.Request, indexUID string) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SearchRequest
	if r.Method == http.MethodGet {
		req.Q = r.URL.Query().Get("q")
	} else {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			a.errorResponse(w, "invalid_search_q", err.Error(), http.StatusBadRequest)
			return
		}
	}

	results, err := a.engine.Search(indexUID, &req)
	if err != nil {
		a.errorResponse(w, "internal", err.Error(), http.StatusInternalServerError)
		return
	}

	a.respond(w, results, http.StatusOK)
}

func (a *API) handleDocuments(w http.ResponseWriter, r *http.Request, indexUID, subPath string) {
	if r.Method == http.MethodPost {
		a.addDocuments(w, r, indexUID)
	} else {
		a.respond(w, []string{}, http.StatusOK)
	}
}

func (a *API) addDocuments(w http.ResponseWriter, r *http.Request, indexUID string) {
	primaryKey := r.URL.Query().Get("primaryKey")
	body, _ := io.ReadAll(r.Body)

	task := a.tasks.Enqueue(TaskTypeDocumentsAdditionOrUpdate, indexUID, map[string]interface{}{
		"primaryKey": primaryKey,
	})

	go func() {
		var docs []map[string]interface{}
		json.Unmarshal(body, &docs)
		a.engine.AddDocuments(indexUID, docs, primaryKey)
		a.tasks.Complete(task.UID, nil)
	}()

	a.respond(w, task.Summarize(), http.StatusAccepted)
}

func (a *API) handleSettings(w http.ResponseWriter, r *http.Request, indexUID, subPath string) {
	settings, _ := a.engine.GetSettings(indexUID)
	a.respond(w, settings, http.StatusOK)
}

// Structs
type SearchRequest struct {
	Q      string      `json:"q"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
	Filter interface{} `json:"filter,omitempty"`
	Facets []string    `json:"facets,omitempty"`
	Sort   []string    `json:"sort,omitempty"`
	Vector []float32   `json:"vector,omitempty"`
}

type SearchResponse struct {
	Hits               []map[string]interface{} `json:"hits"`
	EstimatedTotalHits int                      `json:"estimatedTotalHits"`
	ProcessingTimeMs   int64                    `json:"processingTimeMs"`
	Query              string                   `json:"query"`
}

type IndexSettings struct {
	// Basic settings
	SearchableAttributes []string `json:"searchableAttributes"`
	FilterableAttributes []string `json:"filterableAttributes"`
}

type TaskType string

const (
	TaskTypeDocumentsAdditionOrUpdate TaskType = "documentsAdditionOrUpdate"
)

type Task struct {
	UID        int64                  `json:"uid"`
	IndexUID   string                 `json:"indexUid"`
	Status     string                 `json:"status"`
	Type       TaskType               `json:"type"`
	Details    map[string]interface{} `json:"details,omitempty"`
	Error      *TaskError             `json:"error,omitempty"`
	EnqueuedAt time.Time              `json:"enqueuedAt"`
}

type TaskError struct {
	Message string `json:"message"`
}

func (t *Task) Summarize() map[string]interface{} {
	return map[string]interface{}{
		"taskUid": t.UID,
		"status":  t.Status,
	}
}

func (a *API) respond(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (a *API) errorResponse(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"message": message,
		"code":    code,
	})
}
