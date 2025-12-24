//! LumaDB Rust SDK
//!
//! High-performance client for LumaDB providing:
//! - Kafka-compatible streaming
//! - SQL and LQL queries
//! - Vector similarity search
//! - Document operations
//!
//! ## Compatibility Layers
//!
//! The SDK provides compatibility wrappers for easy migration from other vector databases:
//!
//! - [`compat::qdrant::QdrantClient`] - Qdrant-compatible interface
//! - [`compat::pinecone::PineconeClient`] - Pinecone-compatible interface
//!
//! See the [`compat`] module for examples.

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]

pub mod compat;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// LumaDB client error
#[derive(Error, Debug)]
pub enum LumaError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, LumaError>;

/// LumaDB client
pub struct Client {
    /// Base URL
    base_url: String,
    /// HTTP client
    http: reqwest::Client,
}

impl Client {
    /// Connect to a LumaDB server
    pub async fn connect(address: &str) -> Result<Self> {
        let base_url = if address.starts_with("http") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self { base_url, http })
    }

    /// Execute a SQL or LQL query
    pub async fn query(
        &self,
        query: &str,
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        let url = format!("{}/api/v1/query", self.base_url);

        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({
                "query": query,
                "params": params,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Unknown error").to_string(),
            ));
        }

        let result: QueryResult = response.json().await?;
        Ok(result)
    }

    /// Create a producer for a topic
    pub async fn producer(&self, topic: &str) -> Result<Producer> {
        Ok(Producer {
            client: self,
            topic: topic.to_string(),
        })
    }

    /// Create a consumer for a topic
    pub async fn consumer(&self, topic: &str, group_id: Option<&str>) -> Result<Consumer> {
        Ok(Consumer {
            client: self,
            topic: topic.to_string(),
            group_id: group_id.map(String::from),
            offset: "latest".to_string(),
        })
    }

    /// Insert documents into a collection
    pub async fn insert(
        &self,
        collection: &str,
        docs: &[serde_json::Value],
    ) -> Result<InsertResult> {
        let url = format!("{}/api/v1/collections/{}/documents", self.base_url, collection);

        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "documents": docs }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Insert failed").to_string(),
            ));
        }

        let result: InsertResult = response.json().await?;
        Ok(result)
    }

    /// Find documents in a collection
    pub async fn find(
        &self,
        collection: &str,
        filter: Option<&serde_json::Value>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        let mut url = format!("{}/api/v1/collections/{}/documents", self.base_url, collection);

        if let Some(l) = limit {
            url.push_str(&format!("?limit={}", l));
        }

        let response = self.http.get(&url).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Find failed").to_string(),
            ));
        }

        let docs: Vec<serde_json::Value> = response.json().await?;
        Ok(docs)
    }

    /// Find one document
    pub async fn find_one(
        &self,
        collection: &str,
        filter: Option<&serde_json::Value>,
    ) -> Result<Option<serde_json::Value>> {
        let docs = self.find(collection, filter, Some(1)).await?;
        Ok(docs.into_iter().next())
    }

    /// Update documents
    pub async fn update(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        update: &serde_json::Value,
    ) -> Result<UpdateResult> {
        let url = format!("{}/api/v1/collections/{}/update", self.base_url, collection);

        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({
                "filter": filter,
                "update": update,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Update failed").to_string(),
            ));
        }

        let result: UpdateResult = response.json().await?;
        Ok(result)
    }

    /// Delete documents
    pub async fn delete(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<DeleteResult> {
        let url = format!("{}/api/v1/collections/{}/delete", self.base_url, collection);

        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "filter": filter }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Delete failed").to_string(),
            ));
        }

        let result: DeleteResult = response.json().await?;
        Ok(result)
    }

    /// Vector similarity search
    pub async fn vector_search(
        &self,
        collection: &str,
        vector: &[f32],
        k: usize,
        filter: Option<&str>,
    ) -> Result<Vec<(serde_json::Value, f32)>> {
        let url = format!("{}/api/v1/vectors/search", self.base_url);

        let mut request = serde_json::json!({
            "collection": collection,
            "vector": vector,
            "k": k,
        });

        if let Some(f) = filter {
            request["filter"] = serde_json::Value::String(f.to_string());
        }

        let response = self.http.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Search failed").to_string(),
            ));
        }

        let results: Vec<VectorSearchResult> = response.json().await?;
        Ok(results
            .into_iter()
            .map(|r| (r.document.unwrap_or_default(), r.score))
            .collect())
    }

    /// Close the connection
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Kafka-compatible producer
pub struct Producer<'a> {
    client: &'a Client,
    topic: String,
}

impl<'a> Producer<'a> {
    /// Send a message
    pub async fn send(
        &self,
        value: &serde_json::Value,
        key: Option<&str>,
        partition: Option<i32>,
        headers: Option<&HashMap<String, String>>,
    ) -> Result<RecordMetadata> {
        let url = format!("{}/api/v1/topics/{}/produce", self.client.base_url, self.topic);

        let record = ProduceRecord {
            key: key.map(String::from),
            value: value.clone(),
            headers: headers.cloned(),
            partition,
        };

        let response = self
            .client
            .http
            .post(&url)
            .json(&serde_json::json!({
                "records": [record],
                "acks": 1,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Produce failed").to_string(),
            ));
        }

        let results: Vec<RecordMetadata> = response.json().await?;
        results.into_iter().next().ok_or_else(|| {
            LumaError::Query("No metadata returned".to_string())
        })
    }

    /// Send multiple messages
    pub async fn send_batch(
        &self,
        records: &[(serde_json::Value, Option<String>)],
    ) -> Result<Vec<RecordMetadata>> {
        let url = format!("{}/api/v1/topics/{}/produce", self.client.base_url, self.topic);

        let records: Vec<ProduceRecord> = records
            .iter()
            .map(|(value, key)| ProduceRecord {
                key: key.clone(),
                value: value.clone(),
                headers: None,
                partition: None,
            })
            .collect();

        let response = self
            .client
            .http
            .post(&url)
            .json(&serde_json::json!({
                "records": records,
                "acks": 1,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Produce failed").to_string(),
            ));
        }

        let results: Vec<RecordMetadata> = response.json().await?;
        Ok(results)
    }

    /// Flush pending messages
    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Kafka-compatible consumer
pub struct Consumer<'a> {
    client: &'a Client,
    topic: String,
    group_id: Option<String>,
    offset: String,
}

impl<'a> Consumer<'a> {
    /// Poll for messages
    pub async fn poll(&mut self, timeout_ms: u64, max_records: usize) -> Result<Vec<ConsumeRecord>> {
        let mut url = format!(
            "{}/api/v1/topics/{}/consume?max_records={}",
            self.client.base_url, self.topic, max_records
        );

        if let Some(ref group) = self.group_id {
            url.push_str(&format!("&group_id={}", group));
        }

        url.push_str(&format!("&offset={}", self.offset));

        let response = self.client.http.get(&url).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Consume failed").to_string(),
            ));
        }

        let records: Vec<ConsumeRecord> = response.json().await?;

        // Update offset for next poll
        if let Some(last) = records.last() {
            self.offset = (last.offset + 1).to_string();
        }

        Ok(records)
    }

    /// Commit offsets
    pub async fn commit(&mut self) -> Result<()> {
        // Would commit offsets to server
        Ok(())
    }
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub data: serde_json::Value,
    #[serde(default)]
    pub metadata: QueryMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryMetadata {
    #[serde(default)]
    pub rows_affected: u64,
    #[serde(default)]
    pub execution_time_ms: f64,
    #[serde(default)]
    pub cached: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResult {
    pub inserted_count: u64,
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceRecord {
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub headers: Option<HashMap<String, String>>,
    pub partition: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: serde_json::Value,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub document: Option<serde_json::Value>,
    pub vector: Option<Vec<f32>>,
}
