//! Elasticsearch REST API Protocol Implementation
//! Provides Elasticsearch-compatible HTTP interface with AI-powered search

use warp::Filter;
use std::sync::Arc;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{info, debug, error, warn};
use chrono::{DateTime, Utc};

/// Elasticsearch document storage
pub struct ElasticsearchStore {
    indices: Arc<DashMap<String, Index>>,
    settings: Arc<RwLock<ClusterSettings>>,
}

#[derive(Clone, Debug)]
struct Index {
    name: String,
    documents: Arc<DashMap<String, Document>>,
    mapping: IndexMapping,
    settings: IndexSettings,
    created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
struct IndexMapping {
    properties: HashMap<String, FieldMapping>,
    dynamic: bool,
}

#[derive(Clone, Debug)]
struct FieldMapping {
    field_type: String,
    analyzer: Option<String>,
    index: bool,
}

#[derive(Clone, Debug)]
struct IndexSettings {
    number_of_shards: u32,
    number_of_replicas: u32,
    refresh_interval: String,
}

impl Default for IndexSettings {
    fn default() -> Self {
        Self {
            number_of_shards: 1,
            number_of_replicas: 0,
            refresh_interval: "1s".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Document {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_source")]
    source: Value,
    #[serde(rename = "_version")]
    version: u64,
    #[serde(skip)]
    indexed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
struct ClusterSettings {
    cluster_name: String,
    node_name: String,
}

impl ElasticsearchStore {
    pub fn new() -> Self {
        Self {
            indices: Arc::new(DashMap::new()),
            settings: Arc::new(RwLock::new(ClusterSettings {
                cluster_name: "lumadb-es".to_string(),
                node_name: "lumadb-node-1".to_string(),
            })),
        }
    }

    /// Create index
    pub fn create_index(&self, name: &str, settings: Option<IndexSettings>, mapping: Option<IndexMapping>) -> Result<(), String> {
        if self.indices.contains_key(name) {
            return Err(format!("index [{}] already exists", name));
        }
        self.indices.insert(name.to_string(), Index {
            name: name.to_string(),
            documents: Arc::new(DashMap::new()),
            mapping: mapping.unwrap_or_default(),
            settings: settings.unwrap_or_default(),
            created_at: Utc::now(),
        });
        Ok(())
    }

    /// Delete index
    pub fn delete_index(&self, name: &str) -> Result<(), String> {
        self.indices.remove(name)
            .map(|_| ())
            .ok_or_else(|| format!("index [{}] not found", name))
    }

    /// Index document
    pub fn index_document(&self, index: &str, id: Option<String>, doc: Value) -> Result<Document, String> {
        self.indices.get(index).map(|idx| {
            let doc_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let version = idx.documents.get(&doc_id)
                .map(|d| d.version + 1)
                .unwrap_or(1);
            
            let document = Document {
                id: doc_id.clone(),
                source: doc,
                version,
                indexed_at: Utc::now(),
            };
            idx.documents.insert(doc_id, document.clone());
            document
        }).ok_or_else(|| format!("index [{}] not found", index))
    }

    /// Get document
    pub fn get_document(&self, index: &str, id: &str) -> Option<Document> {
        self.indices.get(index)
            .and_then(|idx| idx.documents.get(id).map(|d| d.clone()))
    }

    /// Delete document
    pub fn delete_document(&self, index: &str, id: &str) -> Result<(), String> {
        self.indices.get(index)
            .ok_or_else(|| format!("index [{}] not found", index))?
            .documents.remove(id)
            .map(|_| ())
            .ok_or_else(|| format!("document [{}] not found", id))
    }

    /// Search documents
    pub fn search(&self, index: &str, query: &SearchQuery) -> SearchResult {
        let mut hits = Vec::new();
        let mut total = 0;
        let mut all_docs: Vec<Document> = Vec::new();

        if let Some(idx) = self.indices.get(index) {
            for doc_ref in idx.documents.iter() {
                let doc = doc_ref.value().clone();
                if self.matches_query(&doc, &query.query) {
                    total += 1;
                    all_docs.push(doc.clone());
                    if hits.len() < query.size {
                        hits.push(SearchHit {
                            index: index.to_string(),
                            id: doc.id.clone(),
                            score: 1.0,
                            source: doc.source.clone(),
                        });
                    }
                }
            }
        }

        // Process aggregations
        let aggregations = query.aggs.as_ref().map(|aggs| {
            self.compute_aggregations(&all_docs, aggs)
        });

        SearchResult {
            took: 1,
            timed_out: false,
            hits: HitsResult {
                total: TotalHits { value: total, relation: "eq".to_string() },
                max_score: hits.first().map(|_| 1.0),
                hits,
            },
            aggregations,
        }
    }

    /// Compute aggregations on matching documents
    fn compute_aggregations(&self, docs: &[Document], aggs: &Value) -> Value {
        let mut result = json!({});
        
        if let Some(aggs_obj) = aggs.as_object() {
            for (agg_name, agg_def) in aggs_obj {
                // Terms aggregation
                if let Some(terms) = agg_def.get("terms") {
                    if let Some(field) = terms.get("field").and_then(|f| f.as_str()) {
                        let size = terms.get("size").and_then(|s| s.as_u64()).unwrap_or(10) as usize;
                        let mut counts: HashMap<String, u64> = HashMap::new();
                        
                        for doc in docs {
                            if let Some(val) = doc.source.get(field) {
                                let key = match val {
                                    Value::String(s) => s.clone(),
                                    Value::Number(n) => n.to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    _ => continue,
                                };
                                *counts.entry(key).or_insert(0) += 1;
                            }
                        }
                        
                        let mut buckets: Vec<_> = counts.into_iter().collect();
                        buckets.sort_by(|a, b| b.1.cmp(&a.1));
                        buckets.truncate(size);
                        
                        result[agg_name] = json!({
                            "buckets": buckets.into_iter().map(|(key, count)| {
                                json!({"key": key, "doc_count": count})
                            }).collect::<Vec<_>>()
                        });
                    }
                }
                // Avg aggregation
                else if let Some(avg_def) = agg_def.get("avg") {
                    if let Some(field) = avg_def.get("field").and_then(|f| f.as_str()) {
                        let (sum, count) = docs.iter().fold((0.0, 0), |(sum, count), doc| {
                            if let Some(Value::Number(n)) = doc.source.get(field) {
                                (sum + n.as_f64().unwrap_or(0.0), count + 1)
                            } else {
                                (sum, count)
                            }
                        });
                        result[agg_name] = json!({"value": if count > 0 { sum / count as f64 } else { 0.0 }});
                    }
                }
                // Sum aggregation
                else if let Some(sum_def) = agg_def.get("sum") {
                    if let Some(field) = sum_def.get("field").and_then(|f| f.as_str()) {
                        let sum: f64 = docs.iter().filter_map(|doc| {
                            doc.source.get(field).and_then(|v| v.as_f64())
                        }).sum();
                        result[agg_name] = json!({"value": sum});
                    }
                }
                // Min aggregation
                else if let Some(min_def) = agg_def.get("min") {
                    if let Some(field) = min_def.get("field").and_then(|f| f.as_str()) {
                        let min_val = docs.iter().filter_map(|doc| {
                            doc.source.get(field).and_then(|v| v.as_f64())
                        }).fold(f64::MAX, f64::min);
                        result[agg_name] = json!({"value": if min_val == f64::MAX { Value::Null } else { json!(min_val) }});
                    }
                }
                // Max aggregation
                else if let Some(max_def) = agg_def.get("max") {
                    if let Some(field) = max_def.get("field").and_then(|f| f.as_str()) {
                        let max_val = docs.iter().filter_map(|doc| {
                            doc.source.get(field).and_then(|v| v.as_f64())
                        }).fold(f64::MIN, f64::max);
                        result[agg_name] = json!({"value": if max_val == f64::MIN { Value::Null } else { json!(max_val) }});
                    }
                }
                // Value count
                else if let Some(count_def) = agg_def.get("value_count") {
                    if let Some(field) = count_def.get("field").and_then(|f| f.as_str()) {
                        let count = docs.iter().filter(|doc| doc.source.get(field).is_some()).count();
                        result[agg_name] = json!({"value": count});
                    }
                }
                // Cardinality
                else if let Some(card_def) = agg_def.get("cardinality") {
                    if let Some(field) = card_def.get("field").and_then(|f| f.as_str()) {
                        let unique: std::collections::HashSet<String> = docs.iter()
                            .filter_map(|doc| doc.source.get(field))
                            .map(|v| v.to_string())
                            .collect();
                        result[agg_name] = json!({"value": unique.len()});
                    }
                }
                // Histogram
                else if let Some(hist_def) = agg_def.get("histogram") {
                    if let Some(field) = hist_def.get("field").and_then(|f| f.as_str()) {
                        let interval = hist_def.get("interval").and_then(|i| i.as_f64()).unwrap_or(10.0);
                        let mut buckets: HashMap<i64, u64> = HashMap::new();
                        
                        for doc in docs {
                            if let Some(Value::Number(n)) = doc.source.get(field) {
                                let val = n.as_f64().unwrap_or(0.0);
                                let bucket_key = ((val / interval).floor() * interval) as i64;
                                *buckets.entry(bucket_key).or_insert(0) += 1;
                            }
                        }
                        
                        let mut sorted_buckets: Vec<_> = buckets.into_iter().collect();
                        sorted_buckets.sort_by_key(|(k, _)| *k);
                        
                        result[agg_name] = json!({
                            "buckets": sorted_buckets.into_iter().map(|(key, count)| {
                                json!({"key": key, "doc_count": count})
                            }).collect::<Vec<_>>()
                        });
                    }
                }
                // Stats aggregation (combined)
                else if let Some(stats_def) = agg_def.get("stats") {
                    if let Some(field) = stats_def.get("field").and_then(|f| f.as_str()) {
                        let values: Vec<f64> = docs.iter()
                            .filter_map(|doc| doc.source.get(field).and_then(|v| v.as_f64()))
                            .collect();
                        
                        if values.is_empty() {
                            result[agg_name] = json!({"count": 0, "min": null, "max": null, "avg": null, "sum": 0});
                        } else {
                            let count = values.len();
                            let sum: f64 = values.iter().sum();
                            let min = values.iter().cloned().fold(f64::MAX, f64::min);
                            let max = values.iter().cloned().fold(f64::MIN, f64::max);
                            result[agg_name] = json!({
                                "count": count,
                                "min": min,
                                "max": max,
                                "avg": sum / count as f64,
                                "sum": sum
                            });
                        }
                    }
                }
            }
        }
        
        result
    }

    /// Check if document matches query
    fn matches_query(&self, doc: &Document, query: &QueryDSL) -> bool {
        match query {
            QueryDSL::MatchAll => true,
            QueryDSL::Match { field, value } => {
                doc.source.get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase().contains(&value.to_lowercase()))
                    .unwrap_or(false)
            }
            QueryDSL::Term { field, value } => {
                doc.source.get(field)
                    .map(|v| v == value)
                    .unwrap_or(false)
            }
            QueryDSL::Range { field, gte, lte, gt, lt } => {
                if let Some(v) = doc.source.get(field) {
                    if let Some(num) = v.as_f64() {
                        let mut ok = true;
                        if let Some(g) = gte { ok &= num >= *g; }
                        if let Some(l) = lte { ok &= num <= *l; }
                        if let Some(g) = gt { ok &= num > *g; }
                        if let Some(l) = lt { ok &= num < *l; }
                        return ok;
                    }
                }
                false
            }
            QueryDSL::Bool { must, should, must_not } => {
                let must_ok = must.as_ref().map(|q| q.iter().all(|qq| self.matches_query(doc, qq))).unwrap_or(true);
                let should_ok = should.as_ref().map(|q| q.is_empty() || q.iter().any(|qq| self.matches_query(doc, qq))).unwrap_or(true);
                let must_not_ok = must_not.as_ref().map(|q| q.iter().all(|qq| !self.matches_query(doc, qq))).unwrap_or(true);
                must_ok && should_ok && must_not_ok
            }
            QueryDSL::Wildcard { field, value } => {
                doc.source.get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| {
                        // Simple wildcard matching without regex
                        // * matches any sequence, ? matches any single char
                        Self::wildcard_match(value, s)
                    })
                    .unwrap_or(false)
            }
            QueryDSL::Prefix { field, value } => {
                doc.source.get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.starts_with(value))
                    .unwrap_or(false)
            }
            QueryDSL::Exists { field } => {
                doc.source.get(field).is_some()
            }
        }
    }

    /// Simple wildcard matching (* matches any sequence, ? matches single char)
    fn wildcard_match(pattern: &str, text: &str) -> bool {
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let text_chars: Vec<char> = text.chars().collect();
        Self::wildcard_match_dp(&pattern_chars, &text_chars, 0, 0)
    }

    fn wildcard_match_dp(pattern: &[char], text: &[char], p: usize, t: usize) -> bool {
        if p == pattern.len() {
            return t == text.len();
        }

        if pattern[p] == '*' {
            // Try matching zero or more characters
            for i in t..=text.len() {
                if Self::wildcard_match_dp(pattern, text, p + 1, i) {
                    return true;
                }
            }
            return false;
        }

        if t < text.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
            return Self::wildcard_match_dp(pattern, text, p + 1, t + 1);
        }

        false
    }

    /// Multi-search
    pub fn msearch(&self, searches: Vec<(String, SearchQuery)>) -> Vec<SearchResult> {
        searches.into_iter()
            .map(|(index, query)| self.search(&index, &query))
            .collect()
    }

    /// Bulk operations
    pub fn bulk(&self, operations: Vec<BulkOperation>) -> BulkResponse {
        let mut items = Vec::new();
        let mut errors = false;

        for op in operations {
            match op {
                BulkOperation::Index { index, id, doc } => {
                    match self.index_document(&index, id.clone(), doc) {
                        Ok(d) => items.push(BulkItem::Index {
                            index,
                            id: d.id,
                            version: d.version,
                            result: "created".to_string(),
                            status: 201,
                        }),
                        Err(e) => {
                            errors = true;
                            items.push(BulkItem::Error { error: e });
                        }
                    }
                }
                BulkOperation::Delete { index, id } => {
                    match self.delete_document(&index, &id) {
                        Ok(_) => items.push(BulkItem::Delete {
                            index,
                            id,
                            result: "deleted".to_string(),
                            status: 200,
                        }),
                        Err(e) => {
                            errors = true;
                            items.push(BulkItem::Error { error: e });
                        }
                    }
                }
            }
        }

        BulkResponse {
            took: 1,
            errors,
            items,
        }
    }
}

// === Query DSL ===

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum QueryDSL {
    MatchAll,
    Match { field: String, value: String },
    Term { field: String, value: Value },
    Range { field: String, gte: Option<f64>, lte: Option<f64>, gt: Option<f64>, lt: Option<f64> },
    Bool { must: Option<Vec<QueryDSL>>, should: Option<Vec<QueryDSL>>, must_not: Option<Vec<QueryDSL>> },
    Wildcard { field: String, value: String },
    Prefix { field: String, value: String },
    Exists { field: String },
}

impl Default for QueryDSL {
    fn default() -> Self {
        QueryDSL::MatchAll
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SearchQuery {
    #[serde(default)]
    pub query: QueryDSL,
    #[serde(default = "default_size")]
    pub size: usize,
    #[serde(default)]
    pub from: usize,
    #[serde(default)]
    pub sort: Vec<Value>,
    #[serde(default)]
    pub _source: Option<Vec<String>>,
    #[serde(default)]
    pub aggs: Option<Value>,
}

fn default_size() -> usize { 10 }

// === Search Results ===

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub took: u64,
    pub timed_out: bool,
    #[serde(rename = "hits")]
    pub hits: HitsResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HitsResult {
    pub total: TotalHits,
    pub max_score: Option<f64>,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TotalHits {
    pub value: usize,
    pub relation: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "_score")]
    pub score: f64,
    #[serde(rename = "_source")]
    pub source: Value,
}

// === Bulk Operations ===

#[derive(Debug, Clone)]
pub enum BulkOperation {
    Index { index: String, id: Option<String>, doc: Value },
    Delete { index: String, id: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct BulkResponse {
    pub took: u64,
    pub errors: bool,
    pub items: Vec<BulkItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum BulkItem {
    Index {
        #[serde(rename = "_index")]
        index: String,
        #[serde(rename = "_id")]
        id: String,
        #[serde(rename = "_version")]
        version: u64,
        result: String,
        status: u16,
    },
    Delete {
        #[serde(rename = "_index")]
        index: String,
        #[serde(rename = "_id")]
        id: String,
        result: String,
        status: u16,
    },
    Error {
        error: String,
    },
}

/// Parse bulk request body
fn parse_bulk_body(body: &str) -> Vec<BulkOperation> {
    let lines: Vec<&str> = body.lines().collect();
    let mut operations = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        if let Ok(action) = serde_json::from_str::<Value>(lines[i]) {
            if let Some(index_action) = action.get("index") {
                let index = index_action.get("_index").and_then(|v| v.as_str()).unwrap_or("_default").to_string();
                let id = index_action.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                i += 1;
                if i < lines.len() {
                    if let Ok(doc) = serde_json::from_str::<Value>(lines[i]) {
                        operations.push(BulkOperation::Index { index, id, doc });
                    }
                }
            } else if let Some(del_action) = action.get("delete") {
                let index = del_action.get("_index").and_then(|v| v.as_str()).unwrap_or("_default").to_string();
                let id = del_action.get("_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                operations.push(BulkOperation::Delete { index, id });
            }
        }
        i += 1;
    }

    operations
}

/// Run Elasticsearch protocol server
pub async fn run(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = Arc::new(ElasticsearchStore::new());

    let store_filter = warp::any().map(move || store.clone());

    // GET / - Cluster info
    let root = warp::path::end()
        .and(warp::get())
        .map(|| {
            warp::reply::json(&json!({
                "name": "lumadb-es",
                "cluster_name": "lumadb",
                "cluster_uuid": "lumadb-uuid-001",
                "version": {
                    "number": "8.11.0",
                    "build_flavor": "lumadb",
                    "build_type": "native",
                    "lucene_version": "9.8.0"
                },
                "tagline": "You Know, for Search (and Observability)"
            }))
        });

    // GET /_cluster/health
    let cluster_health = warp::path!("_cluster" / "health")
        .and(warp::get())
        .and(store_filter.clone())
        .map(|store: Arc<ElasticsearchStore>| {
            warp::reply::json(&json!({
                "cluster_name": "lumadb",
                "status": "green",
                "timed_out": false,
                "number_of_nodes": 1,
                "number_of_data_nodes": 1,
                "active_primary_shards": store.indices.len(),
                "active_shards": store.indices.len(),
                "relocating_shards": 0,
                "initializing_shards": 0,
                "unassigned_shards": 0
            }))
        });

    // GET /_cat/indices
    let cat_indices = warp::path!("_cat" / "indices")
        .and(warp::get())
        .and(store_filter.clone())
        .map(|store: Arc<ElasticsearchStore>| {
            let indices: Vec<Value> = store.indices.iter()
                .map(|idx| json!({
                    "health": "green",
                    "status": "open",
                    "index": idx.name,
                    "uuid": format!("idx-{}", idx.name),
                    "docs.count": idx.documents.len(),
                    "store.size": "1kb"
                }))
                .collect();
            warp::reply::json(&indices)
        });

    // PUT /:index - Create index
    let create_index = warp::path!(String)
        .and(warp::put())
        .and(store_filter.clone())
        .and(warp::body::json().or_else(|_| async { Ok::<_, warp::Rejection>((json!({}),)) }))
        .map(|index: String, store: Arc<ElasticsearchStore>, _body: Value| {
            match store.create_index(&index, None, None) {
                Ok(_) => warp::reply::with_status(
                    warp::reply::json(&json!({"acknowledged": true, "index": index})),
                    warp::http::StatusCode::OK
                ),
                Err(e) => warp::reply::with_status(
                    warp::reply::json(&json!({"error": {"type": "resource_already_exists_exception", "reason": e}})),
                    warp::http::StatusCode::BAD_REQUEST
                ),
            }
        });

    // DELETE /:index - Delete index
    let delete_index = warp::path!(String)
        .and(warp::delete())
        .and(store_filter.clone())
        .map(|index: String, store: Arc<ElasticsearchStore>| {
            match store.delete_index(&index) {
                Ok(_) => warp::reply::with_status(
                    warp::reply::json(&json!({"acknowledged": true})),
                    warp::http::StatusCode::OK
                ),
                Err(e) => warp::reply::with_status(
                    warp::reply::json(&json!({"error": {"type": "index_not_found_exception", "reason": e}})),
                    warp::http::StatusCode::NOT_FOUND
                ),
            }
        });

    // POST /:index/_doc - Index document
    let index_doc = warp::path!(String / "_doc")
        .and(warp::post())
        .and(store_filter.clone())
        .and(warp::body::json())
        .map(|index: String, store: Arc<ElasticsearchStore>, body: Value| {
            // Auto-create index
            let _ = store.create_index(&index, None, None);
            match store.index_document(&index, None, body) {
                Ok(doc) => warp::reply::with_status(
                    warp::reply::json(&json!({
                        "_index": index,
                        "_id": doc.id,
                        "_version": doc.version,
                        "result": "created"
                    })),
                    warp::http::StatusCode::CREATED
                ),
                Err(e) => warp::reply::with_status(
                    warp::reply::json(&json!({"error": e})),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR
                ),
            }
        });

    // PUT /:index/_doc/:id - Index document with ID
    let index_doc_with_id = warp::path!(String / "_doc" / String)
        .and(warp::put())
        .and(store_filter.clone())
        .and(warp::body::json())
        .map(|index: String, id: String, store: Arc<ElasticsearchStore>, body: Value| {
            let _ = store.create_index(&index, None, None);
            match store.index_document(&index, Some(id.clone()), body) {
                Ok(doc) => warp::reply::with_status(
                    warp::reply::json(&json!({
                        "_index": index,
                        "_id": doc.id,
                        "_version": doc.version,
                        "result": if doc.version == 1 { "created" } else { "updated" }
                    })),
                    warp::http::StatusCode::OK
                ),
                Err(e) => warp::reply::with_status(
                    warp::reply::json(&json!({"error": e})),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR
                ),
            }
        });

    // GET /:index/_doc/:id - Get document
    let get_doc = warp::path!(String / "_doc" / String)
        .and(warp::get())
        .and(store_filter.clone())
        .map(|index: String, id: String, store: Arc<ElasticsearchStore>| {
            match store.get_document(&index, &id) {
                Some(doc) => warp::reply::with_status(
                    warp::reply::json(&json!({
                        "_index": index,
                        "_id": doc.id,
                        "_version": doc.version,
                        "found": true,
                        "_source": doc.source
                    })),
                    warp::http::StatusCode::OK
                ),
                None => warp::reply::with_status(
                    warp::reply::json(&json!({"found": false})),
                    warp::http::StatusCode::NOT_FOUND
                ),
            }
        });

    // DELETE /:index/_doc/:id - Delete document
    let delete_doc = warp::path!(String / "_doc" / String)
        .and(warp::delete())
        .and(store_filter.clone())
        .map(|index: String, id: String, store: Arc<ElasticsearchStore>| {
            match store.delete_document(&index, &id) {
                Ok(_) => warp::reply::with_status(
                    warp::reply::json(&json!({"result": "deleted"})),
                    warp::http::StatusCode::OK
                ),
                Err(e) => warp::reply::with_status(
                    warp::reply::json(&json!({"error": e})),
                    warp::http::StatusCode::NOT_FOUND
                ),
            }
        });

    // POST /:index/_search - Search
    let search = warp::path!(String / "_search")
        .and(warp::post())
        .and(store_filter.clone())
        .and(warp::body::json().or_else(|_| async { Ok::<_, warp::Rejection>((SearchQuery::default(),)) }))
        .map(|index: String, store: Arc<ElasticsearchStore>, query: SearchQuery| {
            let result = store.search(&index, &query);
            warp::reply::json(&result)
        });

    // GET /:index/_search - Search (GET)
    let search_get = warp::path!(String / "_search")
        .and(warp::get())
        .and(store_filter.clone())
        .map(|index: String, store: Arc<ElasticsearchStore>| {
            let result = store.search(&index, &SearchQuery::default());
            warp::reply::json(&result)
        });

    // POST /_bulk - Bulk operations
    let bulk = warp::path!("_bulk")
        .and(warp::post())
        .and(store_filter.clone())
        .and(warp::body::bytes())
        .map(|store: Arc<ElasticsearchStore>, body: bytes::Bytes| {
            let body_str = String::from_utf8_lossy(&body);
            let operations = parse_bulk_body(&body_str);
            let result = store.bulk(operations);
            warp::reply::json(&result)
        });

    // POST /_msearch - Multi-search
    let msearch = warp::path!("_msearch")
        .and(warp::post())
        .and(store_filter.clone())
        .and(warp::body::bytes())
        .map(|store: Arc<ElasticsearchStore>, body: bytes::Bytes| {
            let body_str = String::from_utf8_lossy(&body);
            let lines: Vec<&str> = body_str.lines().collect();
            let mut searches = Vec::new();
            let mut i = 0;
            while i + 1 < lines.len() {
                if let Ok(header) = serde_json::from_str::<Value>(lines[i]) {
                    let index = header.get("index").and_then(|v| v.as_str()).unwrap_or("_all").to_string();
                    if let Ok(query) = serde_json::from_str::<SearchQuery>(lines[i + 1]) {
                        searches.push((index, query));
                    }
                }
                i += 2;
            }
            let results = store.msearch(searches);
            warp::reply::json(&json!({"responses": results}))
        });

    let routes = root
        .or(cluster_health)
        .or(cat_indices)
        .or(bulk)
        .or(msearch)
        .or(search)
        .or(search_get)
        .or(index_doc)
        .or(index_doc_with_id)
        .or(get_doc)
        .or(delete_doc)
        .or(create_index)
        .or(delete_index);

    info!("Elasticsearch Protocol Server listening on 0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;

    Ok(())
}
