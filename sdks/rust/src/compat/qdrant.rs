//! Qdrant-compatible SDK interface
//!
//! Provides a Qdrant-like API for LumaDB. Useful for migrating
//! applications from Qdrant or for developers familiar with Qdrant.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{Client, LumaError, Result};

/// Qdrant-compatible client
pub struct QdrantClient {
    inner: Client,
}

impl QdrantClient {
    /// Connect to a LumaDB server using Qdrant-compatible port
    pub async fn connect(url: &str) -> Result<Self> {
        let inner = Client::connect(url).await?;
        Ok(Self { inner })
    }

    /// Create a collection
    pub async fn create_collection(
        &self,
        collection_name: &str,
        config: &CreateCollectionConfig,
    ) -> Result<bool> {
        let url = format!("{}/collections/{}", self.base_url(), collection_name);

        let response = self
            .http()
            .put(&url)
            .json(&serde_json::json!({
                "vectors": {
                    "size": config.vectors.size,
                    "distance": config.vectors.distance.as_str(),
                }
            }))
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    /// Delete a collection
    pub async fn delete_collection(&self, collection_name: &str) -> Result<bool> {
        let url = format!("{}/collections/{}", self.base_url(), collection_name);

        let response = self.http().delete(&url).send().await?;

        Ok(response.status().is_success())
    }

    /// Get collection info
    pub async fn get_collection(&self, collection_name: &str) -> Result<CollectionInfo> {
        let url = format!("{}/collections/{}", self.base_url(), collection_name);

        let response = self.http().get(&url).send().await?;

        if !response.status().is_success() {
            return Err(LumaError::Query("Collection not found".to_string()));
        }

        let result: CollectionInfo = response.json().await?;
        Ok(result)
    }

    /// List all collections
    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let url = format!("{}/collections", self.base_url());

        let response = self.http().get(&url).send().await?;

        if !response.status().is_success() {
            return Err(LumaError::Query("Failed to list collections".to_string()));
        }

        let result: ListCollectionsResult = response.json().await?;
        Ok(result
            .result
            .collections
            .into_iter()
            .map(|c| c.name)
            .collect())
    }

    /// Upsert points
    pub async fn upsert(
        &self,
        collection_name: &str,
        points: &[Point],
        wait: bool,
    ) -> Result<UpdateResult> {
        let url = format!("{}/collections/{}/points", self.base_url(), collection_name);

        let request = UpsertRequest {
            points: points.to_vec(),
            wait: Some(wait),
        };

        let response = self.http().put(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Upsert failed").to_string(),
            ));
        }

        let result: UpdateResult = response.json().await?;
        Ok(result)
    }

    /// Search for nearest neighbors
    pub async fn search(
        &self,
        collection_name: &str,
        vector: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<ScoredPoint>> {
        self.search_with_filter(collection_name, vector, limit, None)
            .await
    }

    /// Search with filter
    pub async fn search_with_filter(
        &self,
        collection_name: &str,
        vector: Vec<f32>,
        limit: usize,
        filter: Option<Filter>,
    ) -> Result<Vec<ScoredPoint>> {
        let url = format!(
            "{}/collections/{}/points/search",
            self.base_url(),
            collection_name
        );

        let mut request = serde_json::json!({
            "vector": vector,
            "limit": limit,
            "with_payload": true,
            "with_vector": false,
        });

        if let Some(f) = filter {
            request["filter"] = serde_json::to_value(f)?;
        }

        let response = self.http().post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Search failed").to_string(),
            ));
        }

        let result: SearchResult = response.json().await?;
        Ok(result.result)
    }

    /// Scroll through points
    pub async fn scroll(
        &self,
        collection_name: &str,
        limit: usize,
        offset: Option<PointId>,
        with_payload: bool,
        with_vector: bool,
    ) -> Result<ScrollResult> {
        let url = format!(
            "{}/collections/{}/points/scroll",
            self.base_url(),
            collection_name
        );

        let mut request = serde_json::json!({
            "limit": limit,
            "with_payload": with_payload,
            "with_vector": with_vector,
        });

        if let Some(off) = offset {
            request["offset"] = serde_json::to_value(off)?;
        }

        let response = self.http().post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Scroll failed").to_string(),
            ));
        }

        let result: ScrollResult = response.json().await?;
        Ok(result)
    }

    /// Get points by IDs
    pub async fn get_points(
        &self,
        collection_name: &str,
        ids: &[PointId],
        with_payload: bool,
        with_vector: bool,
    ) -> Result<Vec<Point>> {
        let url = format!(
            "{}/collections/{}/points",
            self.base_url(),
            collection_name
        );

        let request = serde_json::json!({
            "ids": ids,
            "with_payload": with_payload,
            "with_vector": with_vector,
        });

        let response = self.http().post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Get points failed").to_string(),
            ));
        }

        let result: GetPointsResult = response.json().await?;
        Ok(result.result)
    }

    /// Delete points
    pub async fn delete_points(
        &self,
        collection_name: &str,
        selector: PointsSelector,
        wait: bool,
    ) -> Result<UpdateResult> {
        let url = format!(
            "{}/collections/{}/points/delete",
            self.base_url(),
            collection_name
        );

        let request = serde_json::json!({
            "points": selector,
            "wait": wait,
        });

        let response = self.http().post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let error: serde_json::Value = response.json().await?;
            return Err(LumaError::Query(
                error["error"].as_str().unwrap_or("Delete failed").to_string(),
            ));
        }

        let result: UpdateResult = response.json().await?;
        Ok(result)
    }

    /// Count points
    pub async fn count(&self, collection_name: &str, exact: bool) -> Result<u64> {
        let url = format!(
            "{}/collections/{}/points/count",
            self.base_url(),
            collection_name
        );

        let request = serde_json::json!({
            "exact": exact,
        });

        let response = self.http().post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(LumaError::Query("Count failed".to_string()));
        }

        let result: CountResult = response.json().await?;
        Ok(result.result.count)
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

/// Point ID (can be integer or UUID string)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointId {
    Num(u64),
    Uuid(String),
}

impl From<u64> for PointId {
    fn from(id: u64) -> Self {
        PointId::Num(id)
    }
}

impl From<String> for PointId {
    fn from(id: String) -> Self {
        PointId::Uuid(id)
    }
}

impl From<&str> for PointId {
    fn from(id: &str) -> Self {
        PointId::Uuid(id.to_string())
    }
}

/// Distance metric
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Distance {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

impl Distance {
    fn as_str(&self) -> &'static str {
        match self {
            Distance::Cosine => "Cosine",
            Distance::Euclid => "Euclid",
            Distance::Dot => "Dot",
            Distance::Manhattan => "Manhattan",
        }
    }
}

/// Vector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorParams {
    pub size: usize,
    pub distance: Distance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

impl VectorParams {
    pub fn new(size: usize, distance: Distance) -> Self {
        Self {
            size,
            distance,
            on_disk: None,
        }
    }
}

/// Create collection config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollectionConfig {
    pub vectors: VectorParams,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
}

impl CreateCollectionConfig {
    pub fn new(vectors: VectorParams) -> Self {
        Self {
            vectors,
            hnsw_config: None,
        }
    }
}

/// HNSW index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construct: Option<usize>,
}

/// Point with vector and payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub id: PointId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

impl Point {
    pub fn new(id: impl Into<PointId>) -> Self {
        Self {
            id: id.into(),
            vector: None,
            payload: None,
        }
    }

    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }
}

/// Scored point (search result)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredPoint {
    pub id: PointId,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
}

/// Points selector for delete
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointsSelector {
    Points(Vec<PointId>),
    Filter(Filter),
}

/// Filter for search/delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must: Option<Vec<Condition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should: Option<Vec<Condition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_not: Option<Vec<Condition>>,
}

impl Filter {
    pub fn new() -> Self {
        Self {
            must: None,
            should: None,
            must_not: None,
        }
    }

    pub fn must(mut self, condition: Condition) -> Self {
        self.must.get_or_insert_with(Vec::new).push(condition);
        self
    }

    pub fn should(mut self, condition: Condition) -> Self {
        self.should.get_or_insert_with(Vec::new).push(condition);
        self
    }

    pub fn must_not(mut self, condition: Condition) -> Self {
        self.must_not.get_or_insert_with(Vec::new).push(condition);
        self
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub key: String,
    pub r#match: MatchCondition,
}

impl Condition {
    pub fn matches(key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Self {
            key: key.into(),
            r#match: MatchCondition::Value(value.into()),
        }
    }

    pub fn range(key: impl Into<String>, range: Range) -> Self {
        Self {
            key: key.into(),
            r#match: MatchCondition::Range(range),
        }
    }
}

/// Match condition type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchCondition {
    Value(serde_json::Value),
    Range(Range),
}

/// Range filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Range {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<f64>,
}

// Internal types
#[derive(Debug, Serialize)]
struct UpsertRequest {
    points: Vec<Point>,
    wait: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SearchResult {
    result: Vec<ScoredPoint>,
}

#[derive(Debug, Deserialize)]
pub struct ScrollResult {
    pub result: ScrollResultInner,
}

#[derive(Debug, Deserialize)]
pub struct ScrollResultInner {
    pub points: Vec<Point>,
    pub next_page_offset: Option<PointId>,
}

#[derive(Debug, Deserialize)]
struct GetPointsResult {
    result: Vec<Point>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateResult {
    pub result: UpdateResultInner,
}

#[derive(Debug, Deserialize)]
pub struct UpdateResultInner {
    pub operation_id: u64,
    pub status: String,
}

#[derive(Debug, Deserialize)]
struct CountResult {
    result: CountResultInner,
}

#[derive(Debug, Deserialize)]
struct CountResultInner {
    count: u64,
}

#[derive(Debug, Deserialize)]
pub struct CollectionInfo {
    pub result: CollectionInfoInner,
}

#[derive(Debug, Deserialize)]
pub struct CollectionInfoInner {
    pub status: String,
    pub points_count: u64,
    pub vectors_count: u64,
}

#[derive(Debug, Deserialize)]
struct ListCollectionsResult {
    result: ListCollectionsInner,
}

#[derive(Debug, Deserialize)]
struct ListCollectionsInner {
    collections: Vec<CollectionDesc>,
}

#[derive(Debug, Deserialize)]
struct CollectionDesc {
    name: String,
}
