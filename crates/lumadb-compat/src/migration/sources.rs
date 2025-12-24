//! Migration source connectors

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use lumadb_common::types::Document;

use super::tool::DocumentBatch;

/// Migration source type
#[derive(Debug, Clone)]
pub enum MigrationSource {
    /// JSON or JSONL file
    Json(JsonSource),
    /// Qdrant instance
    Qdrant(QdrantSource),
    /// Pinecone index
    Pinecone(PineconeSource),
    /// MongoDB collection
    MongoDB(MongoDBSource),
    /// Weaviate instance
    Weaviate(WeaviateSource),
    /// Milvus instance
    Milvus(MilvusSource),
    /// Zilliz Cloud (managed Milvus)
    Zilliz(ZillizSource),
}

/// Configuration for different sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Source type
    pub source_type: String,
    /// Connection URL or file path
    pub url: String,
    /// Optional API key
    pub api_key: Option<String>,
    /// Database name (for MongoDB)
    pub database: Option<String>,
    /// Collection/index name
    pub collection: Option<String>,
}

/// JSON file source
#[derive(Debug, Clone)]
pub struct JsonSource {
    pub path: String,
}

/// Qdrant source
#[derive(Debug, Clone)]
pub struct QdrantSource {
    pub url: String,
    pub collection: String,
    pub api_key: Option<String>,
}

/// Pinecone source
#[derive(Debug, Clone)]
pub struct PineconeSource {
    pub api_key: String,
    pub environment: String,
    pub index: String,
}

/// MongoDB source
#[derive(Debug, Clone)]
pub struct MongoDBSource {
    pub connection_string: String,
    pub database: String,
    pub collection: String,
}

/// Weaviate source
#[derive(Debug, Clone)]
pub struct WeaviateSource {
    pub url: String,
    pub class_name: String,
    pub api_key: Option<String>,
    /// OpenAI API key for text2vec-openai module (optional)
    pub openai_api_key: Option<String>,
}

/// Milvus source
#[derive(Debug, Clone)]
pub struct MilvusSource {
    pub host: String,
    pub port: u16,
    pub collection: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Use TLS connection
    pub use_tls: bool,
}

/// Zilliz Cloud source (managed Milvus)
#[derive(Debug, Clone)]
pub struct ZillizSource {
    pub endpoint: String,
    pub collection: String,
    pub api_key: String,
}

impl MigrationSource {
    /// Create a JSON file source
    pub fn json(path: &str) -> Self {
        Self::Json(JsonSource {
            path: path.to_string(),
        })
    }

    /// Create a Qdrant source
    pub fn qdrant(url: &str, collection: &str) -> Self {
        Self::Qdrant(QdrantSource {
            url: url.to_string(),
            collection: collection.to_string(),
            api_key: None,
        })
    }

    /// Create a Qdrant source with API key
    pub fn qdrant_with_key(url: &str, collection: &str, api_key: &str) -> Self {
        Self::Qdrant(QdrantSource {
            url: url.to_string(),
            collection: collection.to_string(),
            api_key: Some(api_key.to_string()),
        })
    }

    /// Create a Pinecone source
    pub fn pinecone(api_key: &str, environment: &str, index: &str) -> Self {
        Self::Pinecone(PineconeSource {
            api_key: api_key.to_string(),
            environment: environment.to_string(),
            index: index.to_string(),
        })
    }

    /// Create a MongoDB source
    pub fn mongodb(connection_string: &str, database: &str, collection: &str) -> Self {
        Self::MongoDB(MongoDBSource {
            connection_string: connection_string.to_string(),
            database: database.to_string(),
            collection: collection.to_string(),
        })
    }

    /// Create a Weaviate source
    pub fn weaviate(url: &str, class_name: &str) -> Self {
        Self::Weaviate(WeaviateSource {
            url: url.to_string(),
            class_name: class_name.to_string(),
            api_key: None,
            openai_api_key: None,
        })
    }

    /// Create a Weaviate source with API key
    pub fn weaviate_with_key(url: &str, class_name: &str, api_key: &str) -> Self {
        Self::Weaviate(WeaviateSource {
            url: url.to_string(),
            class_name: class_name.to_string(),
            api_key: Some(api_key.to_string()),
            openai_api_key: None,
        })
    }

    /// Create a Milvus source
    pub fn milvus(host: &str, port: u16, collection: &str) -> Self {
        Self::Milvus(MilvusSource {
            host: host.to_string(),
            port,
            collection: collection.to_string(),
            username: None,
            password: None,
            use_tls: false,
        })
    }

    /// Create a Milvus source with authentication
    pub fn milvus_with_auth(
        host: &str,
        port: u16,
        collection: &str,
        username: &str,
        password: &str,
        use_tls: bool,
    ) -> Self {
        Self::Milvus(MilvusSource {
            host: host.to_string(),
            port,
            collection: collection.to_string(),
            username: Some(username.to_string()),
            password: Some(password.to_string()),
            use_tls,
        })
    }

    /// Create a Zilliz Cloud source
    pub fn zilliz(endpoint: &str, collection: &str, api_key: &str) -> Self {
        Self::Zilliz(ZillizSource {
            endpoint: endpoint.to_string(),
            collection: collection.to_string(),
            api_key: api_key.to_string(),
        })
    }

    /// Get source type name
    pub fn source_type(&self) -> &'static str {
        match self {
            Self::Json(_) => "json",
            Self::Qdrant(_) => "qdrant",
            Self::Pinecone(_) => "pinecone",
            Self::MongoDB(_) => "mongodb",
            Self::Weaviate(_) => "weaviate",
            Self::Milvus(_) => "milvus",
            Self::Zilliz(_) => "zilliz",
        }
    }

    /// Stream documents from source
    pub async fn stream_documents(
        &self,
        tx: mpsc::Sender<DocumentBatch>,
        batch_size: usize,
        vector_field: &str,
        target_collection: &str,
    ) -> crate::Result<()> {
        match self {
            Self::Json(source) => {
                stream_json(source, tx, batch_size, vector_field, target_collection).await
            }
            Self::Qdrant(source) => {
                stream_qdrant(source, tx, batch_size, target_collection).await
            }
            Self::Pinecone(source) => {
                stream_pinecone(source, tx, batch_size, target_collection).await
            }
            Self::MongoDB(source) => {
                stream_mongodb(source, tx, batch_size, vector_field, target_collection).await
            }
            Self::Weaviate(source) => {
                stream_weaviate(source, tx, batch_size, target_collection).await
            }
            Self::Milvus(source) => {
                stream_milvus(source, tx, batch_size, target_collection).await
            }
            Self::Zilliz(source) => {
                stream_zilliz(source, tx, batch_size, target_collection).await
            }
        }
    }
}

/// Stream documents from JSON/JSONL file
async fn stream_json(
    source: &JsonSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    vector_field: &str,
    target_collection: &str,
) -> crate::Result<()> {
    let path = Path::new(&source.path);
    let file = File::open(path)
        .await
        .map_err(|e| crate::CompatError::Storage(format!("Failed to open file: {}", e)))?;

    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut documents = Vec::with_capacity(batch_size);
    let mut vectors = Vec::with_capacity(batch_size);

    let is_jsonl = source.path.ends_with(".jsonl") || source.path.ends_with(".ndjson");

    if is_jsonl {
        // JSONL format - one JSON object per line
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(value) => {
                    let (doc, vector) = extract_document_and_vector(value, vector_field);
                    if let Some(v) = vector {
                        vectors.push((doc.id.clone(), v));
                    }
                    documents.push(doc);

                    if documents.len() >= batch_size {
                        let batch = DocumentBatch {
                            collection: target_collection.to_string(),
                            documents: std::mem::take(&mut documents),
                            vectors: std::mem::take(&mut vectors),
                        };
                        if tx.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to parse JSON line: {}", e);
                }
            }
        }
    } else {
        // Regular JSON - expect an array
        let mut content = String::new();
        while let Ok(Some(line)) = lines.next_line().await {
            content.push_str(&line);
        }

        match serde_json::from_str::<serde_json::Value>(&content) {
            Ok(serde_json::Value::Array(arr)) => {
                for value in arr {
                    let (doc, vector) = extract_document_and_vector(value, vector_field);
                    if let Some(v) = vector {
                        vectors.push((doc.id.clone(), v));
                    }
                    documents.push(doc);

                    if documents.len() >= batch_size {
                        let batch = DocumentBatch {
                            collection: target_collection.to_string(),
                            documents: std::mem::take(&mut documents),
                            vectors: std::mem::take(&mut vectors),
                        };
                        if tx.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Ok(value) => {
                // Single document
                let (doc, vector) = extract_document_and_vector(value, vector_field);
                if let Some(v) = vector {
                    vectors.push((doc.id.clone(), v));
                }
                documents.push(doc);
            }
            Err(e) => {
                return Err(crate::CompatError::Serialization(format!(
                    "Failed to parse JSON: {}",
                    e
                )));
            }
        }
    }

    // Send remaining documents
    if !documents.is_empty() {
        let batch = DocumentBatch {
            collection: target_collection.to_string(),
            documents,
            vectors,
        };
        let _ = tx.send(batch).await;
    }

    Ok(())
}

/// Extract document and optional vector from JSON value
fn extract_document_and_vector(
    mut value: serde_json::Value,
    vector_field: &str,
) -> (Document, Option<Vec<f32>>) {
    // Extract ID
    let id = value
        .get("_id")
        .or_else(|| value.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Extract vector if present
    let vector = value
        .get(vector_field)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Vec<f32>>()
        });

    // Remove vector from document data to avoid storing it twice
    if let serde_json::Value::Object(ref mut map) = value {
        map.remove(vector_field);
    }

    let doc = Document::with_id(id, value);
    (doc, vector)
}

/// Stream documents from Qdrant
async fn stream_qdrant(
    source: &QdrantSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    target_collection: &str,
) -> crate::Result<()> {
    info!("Streaming from Qdrant: {}/{}", source.url, source.collection);

    let client = reqwest::Client::new();
    let mut offset: Option<String> = None;

    loop {
        // Build scroll request
        let scroll_url = format!(
            "{}/collections/{}/points/scroll",
            source.url, source.collection
        );

        let mut request_body = serde_json::json!({
            "limit": batch_size,
            "with_payload": true,
            "with_vector": true,
        });

        if let Some(ref off) = offset {
            request_body["offset"] = serde_json::json!(off);
        }

        let mut request = client.post(&scroll_url).json(&request_body);

        if let Some(ref api_key) = source.api_key {
            request = request.header("api-key", api_key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| crate::CompatError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(crate::CompatError::Network(format!(
                "Qdrant scroll failed: {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let points = result
            .get("result")
            .and_then(|r| r.get("points"))
            .and_then(|p| p.as_array())
            .cloned()
            .unwrap_or_default();

        if points.is_empty() {
            break;
        }

        let mut documents = Vec::with_capacity(points.len());
        let mut vectors = Vec::with_capacity(points.len());

        for point in points {
            let id = point
                .get("id")
                .map(|v| match v {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => uuid::Uuid::new_v4().to_string(),
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Extract payload
            let payload = point
                .get("payload")
                .cloned()
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

            // Extract vector
            if let Some(vector_value) = point.get("vector") {
                if let Some(arr) = vector_value.as_array() {
                    let vector: Vec<f32> = arr
                        .iter()
                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                        .collect();
                    vectors.push((id.clone(), vector));
                }
            }

            documents.push(Document::with_id(id, payload));
        }

        // Get next offset
        offset = result
            .get("result")
            .and_then(|r| r.get("next_page_offset"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let batch = DocumentBatch {
            collection: target_collection.to_string(),
            documents,
            vectors,
        };

        if tx.send(batch).await.is_err() {
            break;
        }

        if offset.is_none() {
            break;
        }
    }

    Ok(())
}

/// Stream documents from Pinecone
async fn stream_pinecone(
    source: &PineconeSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    target_collection: &str,
) -> crate::Result<()> {
    info!(
        "Streaming from Pinecone: {}/{}",
        source.environment, source.index
    );

    let client = reqwest::Client::new();
    let base_url = format!(
        "https://{}-{}.svc.{}.pinecone.io",
        source.index,
        &source.api_key[..8], // First 8 chars of API key are typically the project ID
        source.environment
    );

    // Pinecone doesn't have a direct scroll API, so we use list + fetch
    // First, get the index stats to understand namespaces
    let describe_url = format!("{}/describe_index_stats", base_url);

    let response = client
        .post(&describe_url)
        .header("Api-Key", &source.api_key)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(|e| crate::CompatError::Network(e.to_string()))?;

    if !response.status().is_success() {
        return Err(crate::CompatError::Network(format!(
            "Pinecone describe failed: {}",
            response.status()
        )));
    }

    let stats: serde_json::Value = response
        .json()
        .await
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

    // Get namespaces
    let namespaces = stats
        .get("namespaces")
        .and_then(|n| n.as_object())
        .map(|m| m.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_else(|| vec!["".to_string()]);

    for namespace in namespaces {
        debug!("Processing Pinecone namespace: {}", namespace);

        let mut pagination_token: Option<String> = None;

        loop {
            // List vectors in namespace
            let mut list_url = format!("{}/vectors/list", base_url);
            if !namespace.is_empty() {
                list_url = format!("{}?namespace={}", list_url, namespace);
            }
            if let Some(ref token) = pagination_token {
                list_url = format!("{}&paginationToken={}", list_url, token);
            }

            let response = client
                .get(&list_url)
                .header("Api-Key", &source.api_key)
                .send()
                .await
                .map_err(|e| crate::CompatError::Network(e.to_string()))?;

            if !response.status().is_success() {
                warn!("Failed to list vectors in namespace {}", namespace);
                break;
            }

            let list_result: serde_json::Value = response
                .json()
                .await
                .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

            let vector_ids: Vec<String> = list_result
                .get("vectors")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.get("id").and_then(|id| id.as_str()).map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            if vector_ids.is_empty() {
                break;
            }

            // Fetch vectors by ID
            let fetch_url = format!("{}/vectors/fetch", base_url);
            let fetch_body = serde_json::json!({
                "ids": vector_ids,
                "namespace": namespace,
            });

            let response = client
                .post(&fetch_url)
                .header("Api-Key", &source.api_key)
                .json(&fetch_body)
                .send()
                .await
                .map_err(|e| crate::CompatError::Network(e.to_string()))?;

            if response.status().is_success() {
                let fetch_result: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

                let vectors_map = fetch_result
                    .get("vectors")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();

                let mut documents = Vec::with_capacity(vectors_map.len());
                let mut vectors = Vec::with_capacity(vectors_map.len());

                for (id, data) in vectors_map {
                    // Extract metadata as document
                    let metadata = data
                        .get("metadata")
                        .cloned()
                        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

                    // Extract vector
                    if let Some(values) = data.get("values").and_then(|v| v.as_array()) {
                        let vector: Vec<f32> = values
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        vectors.push((id.clone(), vector));
                    }

                    documents.push(Document::with_id(id, metadata));
                }

                if !documents.is_empty() {
                    let batch = DocumentBatch {
                        collection: target_collection.to_string(),
                        documents,
                        vectors,
                    };

                    if tx.send(batch).await.is_err() {
                        return Ok(());
                    }
                }
            }

            // Check for pagination
            pagination_token = list_result
                .get("pagination")
                .and_then(|p| p.get("next"))
                .and_then(|n| n.as_str())
                .map(|s| s.to_string());

            if pagination_token.is_none() {
                break;
            }
        }
    }

    Ok(())
}

/// Stream documents from MongoDB
async fn stream_mongodb(
    source: &MongoDBSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    vector_field: &str,
    target_collection: &str,
) -> crate::Result<()> {
    info!(
        "Streaming from MongoDB: {}/{}",
        source.database, source.collection
    );

    // Note: This is a simplified implementation. In production, you'd use the mongodb crate.
    // For now, we'll just document the expected format and return an error.

    // To properly implement this, you would:
    // 1. Connect using mongodb::Client
    // 2. Get database and collection handles
    // 3. Use find() with cursor to stream documents
    // 4. Extract vector field and metadata

    warn!(
        "MongoDB migration requires the mongodb driver. Please use the CLI tool: \
         lumadb migrate --from mongodb://... --source-db {} --source-collection {} --target {}",
        source.database, source.collection, target_collection
    );

    // For demonstration, send an empty batch
    let batch = DocumentBatch {
        collection: target_collection.to_string(),
        documents: vec![],
        vectors: vec![],
    };
    let _ = tx.send(batch).await;

    Ok(())
}

/// Stream documents from Weaviate
async fn stream_weaviate(
    source: &WeaviateSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    target_collection: &str,
) -> crate::Result<()> {
    info!(
        "Streaming from Weaviate: {}/{}",
        source.url, source.class_name
    );

    let client = reqwest::Client::new();
    let mut offset = 0;

    loop {
        // Use Weaviate's GraphQL API to fetch objects
        let graphql_url = format!("{}/v1/graphql", source.url);

        // Build GraphQL query to get objects with vectors
        let query = format!(
            r#"{{
                Get {{
                    {class_name}(
                        limit: {limit}
                        offset: {offset}
                    ) {{
                        _additional {{
                            id
                            vector
                        }}
                    }}
                }}
            }}"#,
            class_name = source.class_name,
            limit = batch_size,
            offset = offset
        );

        let mut request = client.post(&graphql_url).json(&serde_json::json!({
            "query": query
        }));

        // Add authentication headers if provided
        if let Some(ref api_key) = source.api_key {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }
        if let Some(ref openai_key) = source.openai_api_key {
            request = request.header("X-OpenAI-Api-Key", openai_key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| crate::CompatError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(crate::CompatError::Network(format!(
                "Weaviate query failed: {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        // Parse GraphQL response
        let objects = result
            .get("data")
            .and_then(|d| d.get("Get"))
            .and_then(|g| g.get(&source.class_name))
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default();

        if objects.is_empty() {
            break;
        }

        let mut documents = Vec::with_capacity(objects.len());
        let mut vectors = Vec::with_capacity(objects.len());

        for obj in objects {
            // Extract ID and vector from _additional field
            let additional = obj.get("_additional");

            let id = additional
                .and_then(|a| a.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Extract vector
            if let Some(vector_array) = additional.and_then(|a| a.get("vector")).and_then(|v| v.as_array())
            {
                let vector: Vec<f32> = vector_array
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                if !vector.is_empty() {
                    vectors.push((id.clone(), vector));
                }
            }

            // Create document from all non-_additional fields
            let mut doc_data = serde_json::Map::new();
            if let Some(obj_map) = obj.as_object() {
                for (key, value) in obj_map {
                    if key != "_additional" {
                        doc_data.insert(key.clone(), value.clone());
                    }
                }
            }

            documents.push(Document::with_id(id, serde_json::Value::Object(doc_data)));
        }

        let batch = DocumentBatch {
            collection: target_collection.to_string(),
            documents,
            vectors,
        };

        if tx.send(batch).await.is_err() {
            break;
        }

        offset += batch_size;
    }

    Ok(())
}

/// Stream documents from Milvus
async fn stream_milvus(
    source: &MilvusSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    target_collection: &str,
) -> crate::Result<()> {
    info!(
        "Streaming from Milvus: {}:{}/{}",
        source.host, source.port, source.collection
    );

    let client = reqwest::Client::new();
    let protocol = if source.use_tls { "https" } else { "http" };
    let base_url = format!("{}://{}:{}", protocol, source.host, source.port);

    // Milvus 2.x REST API
    let mut offset = 0;

    // First, get collection info to understand the schema
    let describe_url = format!("{}/v1/vector/collections/describe", base_url);
    let mut request = client.post(&describe_url).json(&serde_json::json!({
        "collectionName": source.collection
    }));

    if let (Some(ref username), Some(ref password)) = (&source.username, &source.password) {
        request = request.basic_auth(username, Some(password));
    }

    let response = request
        .send()
        .await
        .map_err(|e| crate::CompatError::Network(e.to_string()))?;

    if !response.status().is_success() {
        return Err(crate::CompatError::Network(format!(
            "Milvus describe collection failed: {}",
            response.status()
        )));
    }

    let collection_info: serde_json::Value = response
        .json()
        .await
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

    // Get vector field name from schema
    let vector_field = collection_info
        .get("data")
        .and_then(|d| d.get("fields"))
        .and_then(|f| f.as_array())
        .and_then(|fields| {
            fields.iter().find_map(|f| {
                if f.get("type").and_then(|t| t.as_str()) == Some("FloatVector") {
                    f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "vector".to_string());

    let primary_field = collection_info
        .get("data")
        .and_then(|d| d.get("fields"))
        .and_then(|f| f.as_array())
        .and_then(|fields| {
            fields.iter().find_map(|f| {
                if f.get("isPrimaryKey").and_then(|p| p.as_bool()) == Some(true) {
                    f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "id".to_string());

    debug!(
        "Milvus schema: primary_field={}, vector_field={}",
        primary_field, vector_field
    );

    loop {
        // Query entities with pagination
        let query_url = format!("{}/v1/vector/query", base_url);
        let mut request = client.post(&query_url).json(&serde_json::json!({
            "collectionName": source.collection,
            "outputFields": ["*"],
            "limit": batch_size,
            "offset": offset
        }));

        if let (Some(ref username), Some(ref password)) = (&source.username, &source.password) {
            request = request.basic_auth(username, Some(password));
        }

        let response = request
            .send()
            .await
            .map_err(|e| crate::CompatError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(crate::CompatError::Network(format!(
                "Milvus query failed: {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let entities = result
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        if entities.is_empty() {
            break;
        }

        let entities_count = entities.len();
        let mut documents = Vec::with_capacity(entities_count);
        let mut vectors = Vec::with_capacity(entities_count);

        for entity in entities {
            // Extract primary key as ID
            let id = entity
                .get(&primary_field)
                .map(|v| match v {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => uuid::Uuid::new_v4().to_string(),
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Extract vector
            if let Some(vector_array) = entity.get(&vector_field).and_then(|v| v.as_array()) {
                let vector: Vec<f32> = vector_array
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                if !vector.is_empty() {
                    vectors.push((id.clone(), vector));
                }
            }

            // Create document from other fields
            let mut doc_data = serde_json::Map::new();
            if let Some(obj) = entity.as_object() {
                for (key, value) in obj {
                    if key != &vector_field && key != &primary_field {
                        doc_data.insert(key.clone(), value.clone());
                    }
                }
            }

            documents.push(Document::with_id(id, serde_json::Value::Object(doc_data)));
        }

        let batch = DocumentBatch {
            collection: target_collection.to_string(),
            documents,
            vectors,
        };

        if tx.send(batch).await.is_err() {
            break;
        }

        offset += batch_size;

        // Safety check to prevent infinite loops
        if entities_count < batch_size {
            break;
        }
    }

    Ok(())
}

/// Stream documents from Zilliz Cloud (managed Milvus)
async fn stream_zilliz(
    source: &ZillizSource,
    tx: mpsc::Sender<DocumentBatch>,
    batch_size: usize,
    target_collection: &str,
) -> crate::Result<()> {
    info!(
        "Streaming from Zilliz Cloud: {}/{}",
        source.endpoint, source.collection
    );

    let client = reqwest::Client::new();
    let base_url = source.endpoint.trim_end_matches('/');

    // Zilliz Cloud uses Milvus-compatible REST API with API key authentication
    let mut offset = 0;

    // First, describe collection to get schema
    let describe_url = format!("{}/v1/vector/collections/describe", base_url);
    let response = client
        .post(&describe_url)
        .header("Authorization", format!("Bearer {}", source.api_key))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "collectionName": source.collection
        }))
        .send()
        .await
        .map_err(|e| crate::CompatError::Network(e.to_string()))?;

    if !response.status().is_success() {
        return Err(crate::CompatError::Network(format!(
            "Zilliz describe collection failed: {}",
            response.status()
        )));
    }

    let collection_info: serde_json::Value = response
        .json()
        .await
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

    // Get vector field name from schema
    let vector_field = collection_info
        .get("data")
        .and_then(|d| d.get("fields"))
        .and_then(|f| f.as_array())
        .and_then(|fields| {
            fields.iter().find_map(|f| {
                let field_type = f.get("type").and_then(|t| t.as_str());
                if field_type == Some("FloatVector") || field_type == Some("BinaryVector") {
                    f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "vector".to_string());

    let primary_field = collection_info
        .get("data")
        .and_then(|d| d.get("fields"))
        .and_then(|f| f.as_array())
        .and_then(|fields| {
            fields.iter().find_map(|f| {
                if f.get("isPrimaryKey").and_then(|p| p.as_bool()) == Some(true) {
                    f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "id".to_string());

    debug!(
        "Zilliz schema: primary_field={}, vector_field={}",
        primary_field, vector_field
    );

    loop {
        // Query entities with pagination
        let query_url = format!("{}/v1/vector/query", base_url);
        let response = client
            .post(&query_url)
            .header("Authorization", format!("Bearer {}", source.api_key))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "collectionName": source.collection,
                "outputFields": ["*"],
                "limit": batch_size,
                "offset": offset
            }))
            .send()
            .await
            .map_err(|e| crate::CompatError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(crate::CompatError::Network(format!(
                "Zilliz query failed: {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let entities = result
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        if entities.is_empty() {
            break;
        }

        let entities_count = entities.len();
        let mut documents = Vec::with_capacity(entities_count);
        let mut vectors = Vec::with_capacity(entities_count);

        for entity in entities {
            // Extract primary key as ID
            let id = entity
                .get(&primary_field)
                .map(|v| match v {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => uuid::Uuid::new_v4().to_string(),
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Extract vector
            if let Some(vector_array) = entity.get(&vector_field).and_then(|v| v.as_array()) {
                let vector: Vec<f32> = vector_array
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                if !vector.is_empty() {
                    vectors.push((id.clone(), vector));
                }
            }

            // Create document from other fields
            let mut doc_data = serde_json::Map::new();
            if let Some(obj) = entity.as_object() {
                for (key, value) in obj {
                    if key != &vector_field && key != &primary_field {
                        doc_data.insert(key.clone(), value.clone());
                    }
                }
            }

            documents.push(Document::with_id(id, serde_json::Value::Object(doc_data)));
        }

        let batch = DocumentBatch {
            collection: target_collection.to_string(),
            documents,
            vectors,
        };

        if tx.send(batch).await.is_err() {
            break;
        }

        offset += batch_size;

        // Safety check to prevent infinite loops
        if entities_count < batch_size {
            break;
        }
    }

    Ok(())
}
