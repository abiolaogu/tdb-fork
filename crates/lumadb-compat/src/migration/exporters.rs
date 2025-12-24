//! Data exporters for migrating data out of LumaDB

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::info;

use lumadb_storage::StorageEngine;

/// Export format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    /// JSON array format
    Json,
    /// JSON Lines format (one document per line)
    JsonL,
    /// Qdrant-compatible format
    Qdrant,
    /// Pinecone-compatible format
    Pinecone,
    /// CSV format (flattened)
    Csv,
}

impl ExportFormat {
    /// Get file extension for format
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::JsonL => "jsonl",
            Self::Qdrant => "qdrant.json",
            Self::Pinecone => "pinecone.json",
            Self::Csv => "csv",
        }
    }
}

/// Data exporter
pub struct Exporter {
    storage: Arc<StorageEngine>,
}

/// Export options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportOptions {
    /// Include vectors in export
    #[serde(default = "default_true")]
    pub include_vectors: bool,

    /// Vector field name in output
    #[serde(default = "default_vector_field")]
    pub vector_field: String,

    /// Pretty print JSON
    #[serde(default)]
    pub pretty: bool,

    /// Batch size for streaming
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_true() -> bool { true }
fn default_vector_field() -> String { "vector".to_string() }
fn default_batch_size() -> usize { 1000 }

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            include_vectors: true,
            vector_field: default_vector_field(),
            pretty: false,
            batch_size: default_batch_size(),
        }
    }
}

/// Export statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportStats {
    pub documents_exported: u64,
    pub vectors_exported: u64,
    pub bytes_written: u64,
}

impl Exporter {
    /// Create a new exporter
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    /// Export a collection to a file
    pub async fn export_to_file(
        &self,
        collection: &str,
        path: &str,
        format: ExportFormat,
        options: ExportOptions,
    ) -> crate::Result<ExportStats> {
        info!("Exporting collection '{}' to {} as {:?}", collection, path, format);

        let file = File::create(Path::new(path))
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Failed to create file: {}", e)))?;

        let mut writer = BufWriter::new(file);
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        // Get all documents
        let documents = self
            .storage
            .scan_documents(collection, None, None)
            .await
            .map_err(|e| crate::CompatError::Storage(e.to_string()))?;

        match format {
            ExportFormat::Json => {
                stats = self
                    .export_json(&mut writer, documents, &options)
                    .await?;
            }
            ExportFormat::JsonL => {
                stats = self
                    .export_jsonl(&mut writer, documents, &options)
                    .await?;
            }
            ExportFormat::Qdrant => {
                stats = self
                    .export_qdrant(&mut writer, documents, &options)
                    .await?;
            }
            ExportFormat::Pinecone => {
                stats = self
                    .export_pinecone(&mut writer, documents, &options)
                    .await?;
            }
            ExportFormat::Csv => {
                stats = self
                    .export_csv(&mut writer, documents, &options)
                    .await?;
            }
        }

        writer
            .flush()
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Failed to flush: {}", e)))?;

        info!(
            "Export complete: {} documents, {} vectors, {} bytes",
            stats.documents_exported, stats.vectors_exported, stats.bytes_written
        );

        Ok(stats)
    }

    /// Export as JSON array
    async fn export_json(
        &self,
        writer: &mut BufWriter<File>,
        documents: Vec<lumadb_common::types::Document>,
        options: &ExportOptions,
    ) -> crate::Result<ExportStats> {
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        let mut docs_array: Vec<serde_json::Value> = Vec::with_capacity(documents.len());

        for doc in documents {
            let mut value = doc.data.clone();

            // Add ID
            if let serde_json::Value::Object(ref mut map) = value {
                map.insert("_id".to_string(), serde_json::Value::String(doc.id.clone()));
            }

            docs_array.push(value);
            stats.documents_exported += 1;
        }

        let json = if options.pretty {
            serde_json::to_string_pretty(&docs_array)
        } else {
            serde_json::to_string(&docs_array)
        }
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let bytes = json.as_bytes();
        stats.bytes_written = bytes.len() as u64;

        writer
            .write_all(bytes)
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

        Ok(stats)
    }

    /// Export as JSON Lines
    async fn export_jsonl(
        &self,
        writer: &mut BufWriter<File>,
        documents: Vec<lumadb_common::types::Document>,
        _options: &ExportOptions,
    ) -> crate::Result<ExportStats> {
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        for doc in documents {
            let mut value = doc.data.clone();

            // Add ID
            if let serde_json::Value::Object(ref mut map) = value {
                map.insert("_id".to_string(), serde_json::Value::String(doc.id.clone()));
            }

            let line = serde_json::to_string(&value)
                .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

            let bytes = format!("{}\n", line);
            stats.bytes_written += bytes.len() as u64;

            writer
                .write_all(bytes.as_bytes())
                .await
                .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

            stats.documents_exported += 1;
        }

        Ok(stats)
    }

    /// Export in Qdrant format
    async fn export_qdrant(
        &self,
        writer: &mut BufWriter<File>,
        documents: Vec<lumadb_common::types::Document>,
        options: &ExportOptions,
    ) -> crate::Result<ExportStats> {
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        #[derive(Serialize)]
        struct QdrantPoint {
            id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            vector: Option<Vec<f32>>,
            payload: serde_json::Value,
        }

        let mut points: Vec<QdrantPoint> = Vec::with_capacity(documents.len());

        for doc in documents {
            // Extract vector from document if present
            let vector = if options.include_vectors {
                doc.data
                    .get(&options.vector_field)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect::<Vec<f32>>()
                    })
            } else {
                None
            };

            if vector.is_some() {
                stats.vectors_exported += 1;
            }

            // Remove vector from payload
            let mut payload = doc.data.clone();
            if let serde_json::Value::Object(ref mut map) = payload {
                map.remove(&options.vector_field);
            }

            points.push(QdrantPoint {
                id: doc.id,
                vector,
                payload,
            });

            stats.documents_exported += 1;
        }

        let output = serde_json::json!({
            "points": points
        });

        let json = if options.pretty {
            serde_json::to_string_pretty(&output)
        } else {
            serde_json::to_string(&output)
        }
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let bytes = json.as_bytes();
        stats.bytes_written = bytes.len() as u64;

        writer
            .write_all(bytes)
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

        Ok(stats)
    }

    /// Export in Pinecone format
    async fn export_pinecone(
        &self,
        writer: &mut BufWriter<File>,
        documents: Vec<lumadb_common::types::Document>,
        options: &ExportOptions,
    ) -> crate::Result<ExportStats> {
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        #[derive(Serialize)]
        struct PineconeVector {
            id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            values: Option<Vec<f32>>,
            metadata: serde_json::Value,
        }

        let mut vectors: Vec<PineconeVector> = Vec::with_capacity(documents.len());

        for doc in documents {
            // Extract vector from document if present
            let values = if options.include_vectors {
                doc.data
                    .get(&options.vector_field)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect::<Vec<f32>>()
                    })
            } else {
                None
            };

            if values.is_some() {
                stats.vectors_exported += 1;
            }

            // Remove vector from metadata
            let mut metadata = doc.data.clone();
            if let serde_json::Value::Object(ref mut map) = metadata {
                map.remove(&options.vector_field);
            }

            vectors.push(PineconeVector {
                id: doc.id,
                values,
                metadata,
            });

            stats.documents_exported += 1;
        }

        let output = serde_json::json!({
            "vectors": vectors,
            "namespace": ""
        });

        let json = if options.pretty {
            serde_json::to_string_pretty(&output)
        } else {
            serde_json::to_string(&output)
        }
        .map_err(|e| crate::CompatError::Serialization(e.to_string()))?;

        let bytes = json.as_bytes();
        stats.bytes_written = bytes.len() as u64;

        writer
            .write_all(bytes)
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

        Ok(stats)
    }

    /// Export as CSV (flattened)
    async fn export_csv(
        &self,
        writer: &mut BufWriter<File>,
        documents: Vec<lumadb_common::types::Document>,
        _options: &ExportOptions,
    ) -> crate::Result<ExportStats> {
        let mut stats = ExportStats {
            documents_exported: 0,
            vectors_exported: 0,
            bytes_written: 0,
        };

        if documents.is_empty() {
            return Ok(stats);
        }

        // Collect all unique keys from all documents
        let mut all_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        all_keys.insert("_id".to_string());

        for doc in &documents {
            if let serde_json::Value::Object(map) = &doc.data {
                for key in map.keys() {
                    all_keys.insert(key.clone());
                }
            }
        }

        let keys: Vec<String> = all_keys.into_iter().collect();

        // Write header
        let header = keys.join(",");
        let header_line = format!("{}\n", header);
        stats.bytes_written += header_line.len() as u64;

        writer
            .write_all(header_line.as_bytes())
            .await
            .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

        // Write rows
        for doc in documents {
            let mut values: Vec<String> = Vec::with_capacity(keys.len());

            for key in &keys {
                let value = if key == "_id" {
                    escape_csv(&doc.id)
                } else if let serde_json::Value::Object(map) = &doc.data {
                    match map.get(key) {
                        Some(serde_json::Value::String(s)) => escape_csv(s),
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(serde_json::Value::Bool(b)) => b.to_string(),
                        Some(serde_json::Value::Null) => String::new(),
                        Some(v) => escape_csv(&v.to_string()),
                        None => String::new(),
                    }
                } else {
                    String::new()
                };

                values.push(value);
            }

            let row = format!("{}\n", values.join(","));
            stats.bytes_written += row.len() as u64;

            writer
                .write_all(row.as_bytes())
                .await
                .map_err(|e| crate::CompatError::Storage(format!("Write error: {}", e)))?;

            stats.documents_exported += 1;
        }

        Ok(stats)
    }
}

/// Escape a string for CSV
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
