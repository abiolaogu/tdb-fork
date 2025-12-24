//! Qdrant REST API request handlers

use std::sync::Arc;
use std::time::Instant;

use actix_web::{web, HttpResponse};
use tracing::{info, debug, error};

use lumadb_storage::StorageEngine;

use super::types::*;
use crate::{CompatError, Result};

/// Shared state for Qdrant handlers
pub struct QdrantState {
    pub storage: Arc<StorageEngine>,
}

// ============================================================================
// Collection Handlers
// ============================================================================

/// GET /collections - List all collections
pub async fn list_collections(
    state: web::Data<QdrantState>,
) -> HttpResponse {
    let start = Instant::now();

    match state.storage.list_collections().await {
        Ok(collections) => {
            let result = CollectionsList {
                collections: collections
                    .into_iter()
                    .map(|c| CollectionDescription { name: c.name })
                    .collect(),
            };
            HttpResponse::Ok().json(QdrantResponse::ok(result, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

/// GET /collections/{name} - Get collection info
pub async fn get_collection(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
) -> HttpResponse {
    let start = Instant::now();
    let name = path.into_inner();

    // Get collection metadata from storage
    match state.storage.get_collection_info(&name).await {
        Ok(Some(info)) => {
            let collection_info = CollectionInfo {
                status: CollectionStatus::Green,
                optimizer_status: OptimizerStatus::Ok,
                vectors_count: info.count as u64,
                indexed_vectors_count: info.count as u64,
                points_count: info.count as u64,
                segments_count: 1,
                config: CollectionConfig {
                    params: CollectionParams {
                        vectors: VectorsConfig::Single(VectorParams {
                            size: info.vector_dimensions.unwrap_or(128),
                            distance: Distance::Cosine,
                            hnsw_config: None,
                            quantization_config: None,
                            on_disk: None,
                        }),
                        shard_number: Some(1),
                        replication_factor: Some(1),
                        write_consistency_factor: Some(1),
                        on_disk_payload: Some(false),
                    },
                    hnsw_config: HnswConfig {
                        m: Some(16),
                        ef_construct: Some(100),
                        full_scan_threshold: Some(10000),
                        max_indexing_threads: Some(0),
                        on_disk: Some(false),
                        payload_m: None,
                    },
                    optimizer_config: OptimizersConfig {
                        deleted_threshold: Some(0.2),
                        vacuum_min_vector_number: Some(1000),
                        default_segment_number: Some(0),
                        max_segment_size: None,
                        memmap_threshold: None,
                        indexing_threshold: Some(20000),
                        flush_interval_sec: Some(5),
                        max_optimization_threads: Some(1),
                    },
                    wal_config: WalConfig {
                        wal_capacity_mb: Some(32),
                        wal_segments_ahead: Some(0),
                    },
                    quantization_config: None,
                },
                payload_schema: std::collections::HashMap::new(),
            };
            HttpResponse::Ok().json(QdrantResponse::ok(collection_info, start.elapsed().as_secs_f64()))
        }
        Ok(None) => {
            error_response(&format!("Collection {} not found", name), start.elapsed().as_secs_f64())
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

/// PUT /collections/{name} - Create collection
pub async fn create_collection(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<CreateCollection>,
) -> HttpResponse {
    let start = Instant::now();
    let name = path.into_inner();

    info!("Creating Qdrant-compatible collection: {}", name);

    // Extract vector dimensions from config
    let dimensions = match &body.vectors {
        VectorsConfig::Single(params) => params.size,
        VectorsConfig::Multi(map) => {
            map.values().next().map(|p| p.size).unwrap_or(128)
        }
    };

    // Create collection with vector support
    match state.storage.create_vector_collection(&name, dimensions).await {
        Ok(_) => {
            HttpResponse::Ok().json(QdrantResponse::ok(true, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

/// DELETE /collections/{name} - Delete collection
pub async fn delete_collection(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
) -> HttpResponse {
    let start = Instant::now();
    let name = path.into_inner();

    match state.storage.delete_collection(&name).await {
        Ok(_) => {
            HttpResponse::Ok().json(QdrantResponse::ok(true, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

// ============================================================================
// Point Handlers
// ============================================================================

/// PUT /collections/{name}/points - Upsert points
pub async fn upsert_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<UpsertPoints>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    debug!("Upserting {} points to collection {}", body.points.len(), collection);

    let mut success_count = 0;

    for point in &body.points {
        let id = point.id.to_string();

        // Extract vector
        let vector = match &point.vector {
            Some(VectorInput::Dense(v)) => v.clone(),
            Some(VectorInput::Named(map)) => {
                map.values().next().cloned().unwrap_or_default()
            }
            None => continue,
        };

        // Create document with vector and payload
        let mut doc = serde_json::json!({
            "_id": id,
            "_vector": vector,
        });

        if let Some(payload) = &point.payload {
            if let serde_json::Value::Object(ref mut doc_map) = doc {
                for (k, v) in payload {
                    doc_map.insert(k.clone(), v.clone());
                }
            }
        }

        let document = lumadb_common::types::Document::with_id(id, doc);

        match state.storage.insert_vector_document(&collection, &document, &vector).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                error!("Failed to upsert point: {}", e);
            }
        }
    }

    let result = UpdateResult {
        operation_id: chrono::Utc::now().timestamp_millis() as u64,
        status: UpdateStatus::Completed,
    };

    HttpResponse::Ok().json(QdrantResponse::ok(result, start.elapsed().as_secs_f64()))
}

/// POST /collections/{name}/points - Get points by IDs
pub async fn get_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<GetPoints>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    let mut points = Vec::new();

    for id in &body.ids {
        let id_str = id.to_string();
        if let Ok(Some(doc)) = state.storage.get_document(&collection, &id_str).await {
            let vector = doc.data.get("_vector")
                .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                .map(VectorInput::Dense);

            let mut payload = doc.data.as_object().cloned().unwrap_or_default();
            payload.remove("_id");
            payload.remove("_vector");

            points.push(PointStruct {
                id: id.clone(),
                vector,
                payload: if payload.is_empty() { None } else { Some(payload.into_iter().collect()) },
            });
        }
    }

    HttpResponse::Ok().json(QdrantResponse::ok(points, start.elapsed().as_secs_f64()))
}

/// POST /collections/{name}/points/delete - Delete points
pub async fn delete_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<DeletePoints>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    match &body.points {
        PointsSelector::Ids(ids) => {
            for id in ids {
                let _ = state.storage.delete_document(&collection, &id.to_string()).await;
            }
        }
        PointsSelector::Filter(_filter) => {
            // TODO: Implement filter-based deletion
        }
    }

    let result = UpdateResult {
        operation_id: chrono::Utc::now().timestamp_millis() as u64,
        status: UpdateStatus::Completed,
    };

    HttpResponse::Ok().json(QdrantResponse::ok(result, start.elapsed().as_secs_f64()))
}

// ============================================================================
// Search Handlers
// ============================================================================

/// POST /collections/{name}/points/search - Search for similar vectors
pub async fn search_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<SearchRequest>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    let vector = body.vector.vector();
    let k = body.limit;

    debug!("Searching {} vectors in collection {}", k, collection);

    match state.storage.vector_search(&collection, vector, k).await {
        Ok(results) => {
            let scored_points: Vec<ScoredPoint> = results
                .into_iter()
                .map(|r| {
                    let id = PointId::Uuid(r.doc_id.clone());
                    let payload = r.payload.as_object().cloned().map(|mut p| {
                        p.remove("_id");
                        p.remove("_vector");
                        p.into_iter().collect()
                    });

                    ScoredPoint {
                        id,
                        version: 1,
                        score: r.score,
                        payload,
                        vector: if body.with_vector.as_ref().map(|w| matches!(w, WithVector::Bool(true))).unwrap_or(false) {
                            r.payload.get("_vector")
                                .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                                .map(VectorInput::Dense)
                        } else {
                            None
                        },
                    }
                })
                .collect();

            HttpResponse::Ok().json(QdrantResponse::ok(scored_points, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

/// POST /collections/{name}/points/scroll - Scroll through points
pub async fn scroll_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    body: web::Json<ScrollRequest>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    let offset = body.offset.as_ref().map(|id| id.to_string());
    let limit = body.limit;

    // Use scan_documents with offset as prefix filter
    let prefix = offset.as_ref().map(|s| s.as_bytes());
    match state.storage.scan_documents(&collection, prefix, Some(limit)).await {
        Ok(docs) => {
            let points: Vec<PointStruct> = docs
                .into_iter()
                .map(|doc| {
                    let id = PointId::Uuid(doc.id.clone());
                    let vector = doc.data.get("_vector")
                        .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                        .map(VectorInput::Dense);

                    let mut payload = doc.data.as_object().cloned().unwrap_or_default();
                    payload.remove("_id");
                    payload.remove("_vector");

                    PointStruct {
                        id,
                        vector,
                        payload: if payload.is_empty() { None } else { Some(payload.into_iter().collect()) },
                    }
                })
                .collect();

            let next_offset = points.last().map(|p| p.id.clone());

            let result = ScrollResult {
                points,
                next_page_offset: next_offset,
            };

            HttpResponse::Ok().json(QdrantResponse::ok(result, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

/// POST /collections/{name}/points/count - Count points
pub async fn count_points(
    state: web::Data<QdrantState>,
    path: web::Path<String>,
    _body: web::Json<CountRequest>,
) -> HttpResponse {
    let start = Instant::now();
    let collection = path.into_inner();

    match state.storage.count_documents(&collection).await {
        Ok(count) => {
            let result = CountResult { count: count as u64 };
            HttpResponse::Ok().json(QdrantResponse::ok(result, start.elapsed().as_secs_f64()))
        }
        Err(e) => error_response(&e.to_string(), start.elapsed().as_secs_f64()),
    }
}

// ============================================================================
// Cluster & Telemetry Handlers
// ============================================================================

/// GET / - Root endpoint
pub async fn root() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "title": "LumaDB Qdrant Compatible API",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /healthz - Health check
pub async fn healthz() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "title": "LumaDB Qdrant Compatible API",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /readyz - Readiness check
pub async fn readyz() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "title": "LumaDB Qdrant Compatible API",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /livez - Liveness check
pub async fn livez() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "title": "LumaDB Qdrant Compatible API",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// GET /telemetry - Telemetry data
pub async fn telemetry() -> HttpResponse {
    let start = Instant::now();
    HttpResponse::Ok().json(QdrantResponse::ok(serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "app": {
            "name": "LumaDB",
            "version": env!("CARGO_PKG_VERSION"),
            "features": {
                "qdrant_compat": true,
            }
        }
    }), start.elapsed().as_secs_f64()))
}

/// GET /cluster - Cluster info
pub async fn cluster_info() -> HttpResponse {
    let start = Instant::now();
    HttpResponse::Ok().json(QdrantResponse::ok(serde_json::json!({
        "status": "disabled",
        "peer_id": 1,
        "peers": {}
    }), start.elapsed().as_secs_f64()))
}

// ============================================================================
// Helper Functions
// ============================================================================

fn error_response(message: &str, time: f64) -> HttpResponse {
    HttpResponse::BadRequest().json(QdrantError {
        status: QdrantErrorStatus {
            error: message.to_string(),
        },
        time,
    })
}
