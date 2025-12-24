//! Pinecone-compatible SDK interface
//!
//! Provides a Pinecone-like API for LumaDB. Useful for migrating
//! applications from Pinecone or for developers familiar with Pinecone.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{Client, LumaError, Result};

/// Pinecone-compatible client
pub struct PineconeClient {
    inner: Client,
    index_name: String,
    api_key: String,
}

impl PineconeClient {
    /// Connect to a LumaDB server using Pinecone-compatible interface
    pub async fn connect(url: &str, api_key: &str, index_name: &str) -> Result<Self> {
        let inner = Client::connect(url).await?;
        Ok(Self {
            inner,
            index_name: index_name.to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// Get index name
    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    /// Upsert vectors
    pub async fn upsert(
        &self,
        vectors: &[Vector],
        namespace: Option<&str>,
    ) -> Result<UpsertResponse> {
        let url = format!("{}/vectors/upsert", self.base_url());

        let request = UpsertRequest {
            vectors: vectors.to_vec(),
            namespace: namespace.map(String::from),
        };

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Upsert failed").to_string(),
            ));
        }

        let result: UpsertResponse = response.json().await?;
        Ok(result)
    }

    /// Query vectors
    pub async fn query(
        &self,
        vector: Vec<f32>,
        top_k: usize,
        namespace: Option<&str>,
    ) -> Result<QueryResponse> {
        self.query_with_filter(vector, top_k, namespace, None, true, false)
            .await
    }

    /// Query with filter
    pub async fn query_with_filter(
        &self,
        vector: Vec<f32>,
        top_k: usize,
        namespace: Option<&str>,
        filter: Option<MetadataFilter>,
        include_metadata: bool,
        include_values: bool,
    ) -> Result<QueryResponse> {
        let url = format!("{}/query", self.base_url());

        let mut request = serde_json::json!({
            "vector": vector,
            "topK": top_k,
            "includeMetadata": include_metadata,
            "includeValues": include_values,
        });

        if let Some(ns) = namespace {
            request["namespace"] = serde_json::Value::String(ns.to_string());
        }

        if let Some(f) = filter {
            request["filter"] = serde_json::to_value(f)?;
        }

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Query failed").to_string(),
            ));
        }

        let result: QueryResponse = response.json().await?;
        Ok(result)
    }

    /// Fetch vectors by IDs
    pub async fn fetch(
        &self,
        ids: &[String],
        namespace: Option<&str>,
    ) -> Result<FetchResponse> {
        let url = format!("{}/vectors/fetch", self.base_url());

        let request = FetchRequest {
            ids: ids.to_vec(),
            namespace: namespace.map(String::from),
        };

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Fetch failed").to_string(),
            ));
        }

        let result: FetchResponse = response.json().await?;
        Ok(result)
    }

    /// Update vector metadata
    pub async fn update(
        &self,
        id: &str,
        values: Option<Vec<f32>>,
        metadata: Option<serde_json::Value>,
        namespace: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/vectors/update", self.base_url());

        let mut request = serde_json::json!({
            "id": id,
        });

        if let Some(v) = values {
            request["values"] = serde_json::Value::Array(
                v.into_iter()
                    .map(|f| serde_json::Value::Number(
                        serde_json::Number::from_f64(f as f64).unwrap_or(serde_json::Number::from(0))
                    ))
                    .collect()
            );
        }

        if let Some(m) = metadata {
            request["setMetadata"] = m;
        }

        if let Some(ns) = namespace {
            request["namespace"] = serde_json::Value::String(ns.to_string());
        }

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Update failed").to_string(),
            ));
        }

        Ok(())
    }

    /// Delete vectors
    pub async fn delete(
        &self,
        ids: Option<&[String]>,
        delete_all: bool,
        filter: Option<MetadataFilter>,
        namespace: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/vectors/delete", self.base_url());

        let mut request = serde_json::json!({});

        if let Some(id_list) = ids {
            request["ids"] = serde_json::to_value(id_list)?;
        }

        if delete_all {
            request["deleteAll"] = serde_json::Value::Bool(true);
        }

        if let Some(f) = filter {
            request["filter"] = serde_json::to_value(f)?;
        }

        if let Some(ns) = namespace {
            request["namespace"] = serde_json::Value::String(ns.to_string());
        }

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Delete failed").to_string(),
            ));
        }

        Ok(())
    }

    /// Get index statistics
    pub async fn describe_index_stats(&self) -> Result<IndexStats> {
        let url = format!("{}/describe_index_stats", self.base_url());

        let response = self
            .http()
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&serde_json::json!({}))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"]
                    .as_str()
                    .unwrap_or("Describe failed")
                    .to_string(),
            ));
        }

        let result: IndexStats = response.json().await?;
        Ok(result)
    }

    /// List vector IDs
    pub async fn list(
        &self,
        prefix: Option<&str>,
        namespace: Option<&str>,
        limit: Option<usize>,
        pagination_token: Option<&str>,
    ) -> Result<ListResponse> {
        let mut url = format!("{}/vectors/list", self.base_url());
        let mut params: Vec<String> = Vec::new();

        if let Some(p) = prefix {
            params.push(format!("prefix={}", p));
        }
        if let Some(ns) = namespace {
            params.push(format!("namespace={}", ns));
        }
        if let Some(l) = limit {
            params.push(format!("limit={}", l));
        }
        if let Some(t) = pagination_token {
            params.push(format!("paginationToken={}", t));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        let response = self
            .http()
            .get(&url)
            .header("Api-Key", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("List failed").to_string(),
            ));
        }

        let result: ListResponse = response.json().await?;
        Ok(result)
    }

    fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    fn http(&self) -> &reqwest::Client {
        &self.inner.http
    }
}

// ============================================================================
// Types
// ============================================================================

/// Vector with ID, values, and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vector {
    pub id: String,
    pub values: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
}

impl Vector {
    pub fn new(id: impl Into<String>, values: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            values,
            metadata: None,
            sparse_values: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_sparse_values(mut self, indices: Vec<u32>, values: Vec<f32>) -> Self {
        self.sparse_values = Some(SparseValues { indices, values });
        self
    }
}

/// Sparse vector values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseValues {
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

/// Scored vector (query result)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredVector {
    pub id: String,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
}

/// Metadata filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataFilter {
    #[serde(flatten)]
    pub conditions: HashMap<String, FilterValue>,
}

impl MetadataFilter {
    pub fn new() -> Self {
        Self {
            conditions: HashMap::new(),
        }
    }

    pub fn eq(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Eq(value.into()),
        );
        self
    }

    pub fn ne(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Ne(value.into()),
        );
        self
    }

    pub fn gt(mut self, key: impl Into<String>, value: f64) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Gt(value),
        );
        self
    }

    pub fn gte(mut self, key: impl Into<String>, value: f64) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Gte(value),
        );
        self
    }

    pub fn lt(mut self, key: impl Into<String>, value: f64) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Lt(value),
        );
        self
    }

    pub fn lte(mut self, key: impl Into<String>, value: f64) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Lte(value),
        );
        self
    }

    pub fn in_list(mut self, key: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::In(values),
        );
        self
    }

    pub fn not_in(mut self, key: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        self.conditions.insert(
            key.into(),
            FilterValue::Nin(values),
        );
        self
    }
}

impl Default for MetadataFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter value types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FilterValue {
    #[serde(rename = "$eq")]
    Eq(serde_json::Value),
    #[serde(rename = "$ne")]
    Ne(serde_json::Value),
    #[serde(rename = "$gt")]
    Gt(f64),
    #[serde(rename = "$gte")]
    Gte(f64),
    #[serde(rename = "$lt")]
    Lt(f64),
    #[serde(rename = "$lte")]
    Lte(f64),
    #[serde(rename = "$in")]
    In(Vec<serde_json::Value>),
    #[serde(rename = "$nin")]
    Nin(Vec<serde_json::Value>),
}

/// Upsert request
#[derive(Debug, Serialize)]
struct UpsertRequest {
    vectors: Vec<Vector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
}

/// Upsert response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertResponse {
    pub upserted_count: u64,
}

/// Query response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub matches: Vec<ScoredVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Fetch request
#[derive(Debug, Serialize)]
struct FetchRequest {
    ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
}

/// Fetch response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub vectors: HashMap<String, Vector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    pub dimension: usize,
    pub index_fullness: f64,
    pub total_vector_count: u64,
    #[serde(default)]
    pub namespaces: HashMap<String, NamespaceStats>,
}

/// Namespace statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStats {
    pub vector_count: u64,
}

/// List response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub vectors: Vec<VectorId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Pagination>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Vector ID in list response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorId {
    pub id: String,
}

/// Pagination info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}
