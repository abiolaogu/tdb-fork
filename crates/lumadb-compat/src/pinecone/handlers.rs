//! Pinecone REST API request handlers

use std::sync::Arc;
use std::collections::HashMap;

use actix_web::{web, HttpResponse};
use tracing::{info, debug, error};

use lumadb_storage::StorageEngine;

use super::types::*;

/// Shared state for Pinecone handlers
pub struct PineconeState {
    pub storage: Arc<StorageEngine>,
    pub index_name: String,
    pub dimension: usize,
}

// ============================================================================
// Vector Handlers
// ============================================================================

/// POST /vectors/upsert - Upsert vectors
pub async fn upsert(
    state: web::Data<PineconeState>,
    body: web::Json<UpsertRequest>,
) -> HttpResponse {
    let namespace = body.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);

    debug!("Upserting {} vectors to {}", body.vectors.len(), collection);

    // Ensure collection exists
    let _ = state.storage.create_vector_collection(&collection, state.dimension).await;

    let mut upserted = 0u64;

    for vector in &body.vectors {
        let mut doc = serde_json::json!({
            "_id": vector.id,
            "_vector": vector.values,
        });

        if let Some(metadata) = &vector.metadata {
            if let serde_json::Value::Object(ref mut doc_map) = doc {
                for (k, v) in metadata {
                    doc_map.insert(k.clone(), v.clone());
                }
            }
        }

        let document = lumadb_common::types::Document::with_id(vector.id.clone(), doc);

        match state.storage.insert_vector_document(&collection, &document, &vector.values).await {
            Ok(_) => upserted += 1,
            Err(e) => {
                error!("Failed to upsert vector {}: {}", vector.id, e);
            }
        }
    }

    HttpResponse::Ok().json(UpsertResponse {
        upserted_count: upserted,
    })
}

/// POST /query - Query vectors
pub async fn query(
    state: web::Data<PineconeState>,
    body: web::Json<QueryRequest>,
) -> HttpResponse {
    let namespace = body.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);

    // Get query vector
    let query_vector = match (&body.vector, &body.id) {
        (Some(v), _) => v.clone(),
        (None, Some(id)) => {
            // Fetch vector by ID
            match state.storage.get_document(&collection, id).await {
                Ok(Some(doc)) => {
                    doc.data.get("_vector")
                        .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                        .unwrap_or_default()
                }
                _ => {
                    return HttpResponse::BadRequest().json(PineconeError::not_found(
                        &format!("Vector with id '{}' not found", id)
                    ));
                }
            }
        }
        (None, None) => {
            return HttpResponse::BadRequest().json(PineconeError::invalid_argument(
                "Either 'vector' or 'id' must be provided"
            ));
        }
    };

    debug!("Querying {} vectors from {}", body.top_k, collection);

    match state.storage.vector_search(&collection, &query_vector, body.top_k).await {
        Ok(results) => {
            let matches: Vec<ScoredVector> = results
                .into_iter()
                .map(|r| {
                    let values = if body.include_values {
                        r.payload.get("_vector")
                            .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                    } else {
                        None
                    };

                    let metadata = if body.include_metadata {
                        let mut meta = r.payload.as_object().cloned().unwrap_or_default();
                        meta.remove("_id");
                        meta.remove("_vector");
                        if meta.is_empty() { None } else { Some(meta.into_iter().collect()) }
                    } else {
                        None
                    };

                    ScoredVector {
                        id: r.doc_id,
                        score: r.score,
                        values,
                        sparse_values: None,
                        metadata,
                    }
                })
                .collect();

            HttpResponse::Ok().json(QueryResponse {
                matches,
                namespace: Some(namespace.to_string()),
                usage: Some(Usage { read_units: 1 }),
            })
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(PineconeError::internal(&e.to_string()))
        }
    }
}

/// GET /vectors/fetch - Fetch vectors by ID
pub async fn fetch(
    state: web::Data<PineconeState>,
    query: web::Query<FetchQueryParams>,
) -> HttpResponse {
    let namespace = query.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);

    let ids: Vec<&str> = query.ids.split(',').collect();
    let mut vectors = HashMap::new();

    for id in ids {
        if let Ok(Some(doc)) = state.storage.get_document(&collection, id).await {
            let values = doc.data.get("_vector")
                .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                .unwrap_or_default();

            let mut metadata = doc.data.as_object().cloned().unwrap_or_default();
            metadata.remove("_id");
            metadata.remove("_vector");

            vectors.insert(id.to_string(), FetchedVector {
                id: id.to_string(),
                values,
                sparse_values: None,
                metadata: if metadata.is_empty() { None } else { Some(metadata.into_iter().collect()) },
            });
        }
    }

    HttpResponse::Ok().json(FetchResponse {
        vectors,
        namespace: Some(namespace.to_string()),
        usage: Some(Usage { read_units: 1 }),
    })
}

/// Query parameters for fetch
#[derive(Debug, serde::Deserialize)]
pub struct FetchQueryParams {
    pub ids: String,
    pub namespace: Option<String>,
}

/// POST /vectors/delete - Delete vectors
pub async fn delete(
    state: web::Data<PineconeState>,
    body: web::Json<DeleteRequest>,
) -> HttpResponse {
    let namespace = body.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);

    if body.delete_all {
        // Delete entire collection
        let _ = state.storage.delete_collection(&collection).await;
    } else if let Some(ids) = &body.ids {
        for id in ids {
            let _ = state.storage.delete_document(&collection, id).await;
        }
    }
    // TODO: Handle filter-based deletion

    HttpResponse::Ok().json(DeleteResponse {})
}

/// POST /vectors/update - Update a vector
pub async fn update(
    state: web::Data<PineconeState>,
    body: web::Json<UpdateRequest>,
) -> HttpResponse {
    let namespace = body.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);

    // Fetch existing vector
    match state.storage.get_document(&collection, &body.id).await {
        Ok(Some(mut doc)) => {
            // Update values if provided
            if let Some(values) = &body.values {
                doc.data["_vector"] = serde_json::json!(values);
            }

            // Update metadata if provided
            if let Some(metadata) = &body.set_metadata {
                if let serde_json::Value::Object(ref mut doc_map) = doc.data {
                    for (k, v) in metadata {
                        doc_map.insert(k.clone(), v.clone());
                    }
                }
            }

            let vector = doc.data.get("_vector")
                .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok())
                .unwrap_or_default();

            let _ = state.storage.insert_vector_document(&collection, &doc, &vector).await;

            HttpResponse::Ok().json(UpdateResponse {})
        }
        Ok(None) => {
            HttpResponse::NotFound().json(PineconeError::not_found(
                &format!("Vector with id '{}' not found", body.id)
            ))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(PineconeError::internal(&e.to_string()))
        }
    }
}

/// GET /describe_index_stats - Get index statistics
pub async fn describe_index_stats(
    state: web::Data<PineconeState>,
) -> HttpResponse {
    let mut namespaces = HashMap::new();
    let mut total_count = 0u64;

    // Get stats for all namespaces (collections with index prefix)
    if let Ok(collections) = state.storage.list_collections().await {
        for coll in collections {
            if coll.name.starts_with(&state.index_name) {
                let ns = coll.name
                    .strip_prefix(&format!("{}_", state.index_name))
                    .unwrap_or("default")
                    .to_string();

                let count = coll.count as u64;
                total_count += count;

                namespaces.insert(ns, NamespaceStats { vector_count: count });
            }
        }
    }

    // Ensure default namespace exists
    if namespaces.is_empty() {
        namespaces.insert("default".to_string(), NamespaceStats { vector_count: 0 });
    }

    HttpResponse::Ok().json(DescribeIndexStatsResponse {
        namespaces,
        dimension: state.dimension,
        index_fullness: 0.0,
        total_vector_count: total_count,
    })
}

/// GET /vectors/list - List vector IDs
pub async fn list(
    state: web::Data<PineconeState>,
    query: web::Query<ListQueryParams>,
) -> HttpResponse {
    let namespace = query.namespace.as_deref().unwrap_or("default");
    let collection = format!("{}_{}", state.index_name, namespace);
    let limit = query.limit.unwrap_or(100);

    let prefix = query.pagination_token.as_ref().map(|s| s.as_bytes());
    match state.storage.scan_documents(&collection, prefix, Some(limit)).await {
        Ok(docs) => {
            let vectors: Vec<VectorId> = docs
                .iter()
                .filter(|d| {
                    if let Some(prefix) = &query.prefix {
                        d.id.starts_with(prefix)
                    } else {
                        true
                    }
                })
                .map(|d| VectorId { id: d.id.clone() })
                .collect();

            let next_token = if vectors.len() == limit {
                docs.last().map(|d| d.id.clone())
            } else {
                None
            };

            HttpResponse::Ok().json(ListResponse {
                vectors,
                pagination: next_token.map(|n| Pagination { next: Some(n) }),
                namespace: Some(namespace.to_string()),
                usage: Some(Usage { read_units: 1 }),
            })
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(PineconeError::internal(&e.to_string()))
        }
    }
}

/// Query parameters for list
#[derive(Debug, serde::Deserialize)]
pub struct ListQueryParams {
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub pagination_token: Option<String>,
    pub namespace: Option<String>,
}
