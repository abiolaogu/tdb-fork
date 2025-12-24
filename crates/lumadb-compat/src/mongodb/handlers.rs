//! MongoDB command handlers

use std::sync::Arc;

use bson::{doc, Bson};
use tracing::{debug, info, error};

use lumadb_storage::StorageEngine;

use super::types::*;
use super::protocol::{bson_to_json, json_to_bson};

/// MongoDB command handler state
pub struct MongoDBState {
    pub storage: Arc<StorageEngine>,
}

impl MongoDBState {
    /// Handle a MongoDB command
    pub async fn handle_command(&self, cmd: bson::Document) -> bson::Document {
        debug!("Handling MongoDB command: {:?}", cmd.keys().collect::<Vec<_>>());

        // Detect command type and dispatch
        if cmd.contains_key("isMaster") || cmd.contains_key("ismaster") {
            return self.handle_is_master().await;
        }
        if cmd.contains_key("hello") {
            return self.handle_hello().await;
        }
        if cmd.contains_key("ping") {
            return self.handle_ping().await;
        }
        if cmd.contains_key("listDatabases") {
            return self.handle_list_databases().await;
        }
        if cmd.contains_key("listCollections") {
            return self.handle_list_collections(&cmd).await;
        }
        if cmd.contains_key("create") {
            return self.handle_create(&cmd).await;
        }
        if cmd.contains_key("drop") {
            return self.handle_drop(&cmd).await;
        }
        if cmd.contains_key("insert") {
            return self.handle_insert(&cmd).await;
        }
        if cmd.contains_key("find") {
            return self.handle_find(&cmd).await;
        }
        if cmd.contains_key("update") {
            return self.handle_update(&cmd).await;
        }
        if cmd.contains_key("delete") {
            return self.handle_delete(&cmd).await;
        }
        if cmd.contains_key("aggregate") {
            return self.handle_aggregate(&cmd).await;
        }
        if cmd.contains_key("createIndexes") {
            return self.handle_create_indexes(&cmd).await;
        }
        if cmd.contains_key("count") {
            return self.handle_count(&cmd).await;
        }
        if cmd.contains_key("getLastError") {
            return self.handle_get_last_error().await;
        }
        if cmd.contains_key("buildInfo") {
            return self.handle_build_info().await;
        }
        if cmd.contains_key("whatsmyuri") {
            return doc! { "ok": 1.0, "you": "127.0.0.1:27017" };
        }
        if cmd.contains_key("saslStart") || cmd.contains_key("saslContinue") {
            return self.handle_sasl(&cmd).await;
        }

        // Unknown command
        doc! {
            "ok": 0.0,
            "errmsg": format!("Unknown command: {:?}", cmd.keys().next()),
            "code": 59
        }
    }

    async fn handle_is_master(&self) -> bson::Document {
        doc! {
            "ok": 1.0,
            "ismaster": true,
            "maxBsonObjectSize": 16777216_i32,
            "maxMessageSizeBytes": 48000000_i32,
            "maxWriteBatchSize": 100000_i32,
            "localTime": bson::DateTime::now(),
            "minWireVersion": 0_i32,
            "maxWireVersion": 17_i32,
            "readOnly": false,
        }
    }

    async fn handle_hello(&self) -> bson::Document {
        doc! {
            "ok": 1.0,
            "isWritablePrimary": true,
            "maxBsonObjectSize": 16777216_i32,
            "maxMessageSizeBytes": 48000000_i32,
            "maxWriteBatchSize": 100000_i32,
            "localTime": bson::DateTime::now(),
            "minWireVersion": 0_i32,
            "maxWireVersion": 17_i32,
            "readOnly": false,
        }
    }

    async fn handle_ping(&self) -> bson::Document {
        doc! { "ok": 1.0 }
    }

    async fn handle_list_databases(&self) -> bson::Document {
        let databases = vec![
            doc! { "name": "admin", "sizeOnDisk": 0_i64, "empty": true },
            doc! { "name": "local", "sizeOnDisk": 0_i64, "empty": true },
            doc! { "name": "lumadb", "sizeOnDisk": 0_i64, "empty": false },
        ];

        doc! {
            "ok": 1.0,
            "databases": databases,
            "totalSize": 0_i64,
        }
    }

    async fn handle_list_collections(&self, _cmd: &bson::Document) -> bson::Document {
        match self.storage.list_collections().await {
            Ok(collections) => {
                let cursor_docs: Vec<bson::Document> = collections
                    .into_iter()
                    .map(|c| {
                        doc! {
                            "name": c.name,
                            "type": "collection",
                            "options": {},
                            "info": { "readOnly": false },
                            "idIndex": {
                                "v": 2_i32,
                                "key": { "_id": 1_i32 },
                                "name": "_id_"
                            }
                        }
                    })
                    .collect();

                doc! {
                    "ok": 1.0,
                    "cursor": {
                        "id": 0_i64,
                        "ns": "lumadb.$cmd.listCollections",
                        "firstBatch": cursor_docs
                    }
                }
            }
            Err(e) => {
                doc! {
                    "ok": 0.0,
                    "errmsg": e.to_string(),
                    "code": 1
                }
            }
        }
    }

    async fn handle_create(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("create").unwrap_or_default();
        info!("Creating MongoDB collection: {}", collection);

        match self.storage.create_collection(collection).await {
            Ok(_) => doc! { "ok": 1.0 },
            Err(e) => doc! {
                "ok": 0.0,
                "errmsg": e.to_string(),
                "code": 1
            },
        }
    }

    async fn handle_drop(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("drop").unwrap_or_default();

        match self.storage.delete_collection(collection).await {
            Ok(_) => doc! { "ok": 1.0 },
            Err(e) => doc! {
                "ok": 0.0,
                "errmsg": e.to_string(),
                "code": 1
            },
        }
    }

    async fn handle_insert(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("insert").unwrap_or_default();
        let documents = match cmd.get_array("documents") {
            Ok(docs) => docs,
            Err(_) => {
                return doc! {
                    "ok": 0.0,
                    "errmsg": "Missing documents field",
                    "code": 2
                };
            }
        };

        let mut inserted = 0i64;

        for doc_bson in documents {
            if let Bson::Document(doc) = doc_bson {
                let json_value = bson_to_json(doc);
                let id = doc
                    .get("_id")
                    .map(|v| match v {
                        Bson::ObjectId(oid) => oid.to_hex(),
                        Bson::String(s) => s.clone(),
                        _ => uuid::Uuid::new_v4().to_string(),
                    })
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

                let document = lumadb_common::types::Document::with_id(id, json_value);

                match self.storage.insert_document(collection, &document).await {
                    Ok(_) => inserted += 1,
                    Err(e) => {
                        error!("Insert error: {}", e);
                    }
                }
            }
        }

        doc! {
            "ok": 1.0,
            "n": inserted
        }
    }

    async fn handle_find(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("find").unwrap_or_default();
        let limit = cmd.get_i64("limit").unwrap_or(100) as usize;
        let db = cmd.get_str("$db").unwrap_or("lumadb");

        match self.storage.scan_documents(collection, None, Some(limit)).await {
            Ok(docs) => {
                let bson_docs: Vec<bson::Document> = docs
                    .into_iter()
                    .map(|d| json_to_bson(&d.data))
                    .collect();

                doc! {
                    "ok": 1.0,
                    "cursor": {
                        "id": 0_i64,
                        "ns": format!("{}.{}", db, collection),
                        "firstBatch": bson_docs
                    }
                }
            }
            Err(e) => {
                doc! {
                    "ok": 0.0,
                    "errmsg": e.to_string(),
                    "code": 1
                }
            }
        }
    }

    async fn handle_update(&self, _cmd: &bson::Document) -> bson::Document {
        // TODO: Implement full update support
        doc! {
            "ok": 1.0,
            "n": 0_i64,
            "nModified": 0_i64
        }
    }

    async fn handle_delete(&self, _cmd: &bson::Document) -> bson::Document {
        // TODO: Implement full delete support
        doc! {
            "ok": 1.0,
            "n": 0_i64
        }
    }

    async fn handle_aggregate(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("aggregate").unwrap_or_default();
        let pipeline = match cmd.get_array("pipeline") {
            Ok(p) => p,
            Err(_) => {
                return doc! {
                    "ok": 0.0,
                    "errmsg": "Missing pipeline",
                    "code": 2
                };
            }
        };
        let db = cmd.get_str("$db").unwrap_or("lumadb");

        // Check for $vectorSearch stage
        for stage in pipeline {
            if let Bson::Document(stage_doc) = stage {
                if let Ok(vector_search) = stage_doc.get_document("$vectorSearch") {
                    return self
                        .handle_vector_search(collection, vector_search, db)
                        .await;
                }
            }
        }

        // Regular aggregation (simplified - just return documents)
        match self.storage.scan_documents(collection, None, Some(100)).await {
            Ok(docs) => {
                let bson_docs: Vec<bson::Document> = docs
                    .into_iter()
                    .map(|d| json_to_bson(&d.data))
                    .collect();

                doc! {
                    "ok": 1.0,
                    "cursor": {
                        "id": 0_i64,
                        "ns": format!("{}.{}", db, collection),
                        "firstBatch": bson_docs
                    }
                }
            }
            Err(e) => {
                doc! {
                    "ok": 0.0,
                    "errmsg": e.to_string(),
                    "code": 1
                }
            }
        }
    }

    /// Handle $vectorSearch aggregation stage (Atlas Vector Search compatible)
    async fn handle_vector_search(
        &self,
        collection: &str,
        vector_search: &bson::Document,
        db: &str,
    ) -> bson::Document {
        info!("Executing $vectorSearch on {}", collection);

        // Extract parameters
        let path = vector_search.get_str("path").unwrap_or("embedding");
        let num_candidates = vector_search.get_i32("numCandidates").unwrap_or(100);
        let limit = vector_search.get_i32("limit").unwrap_or(10);

        // Extract query vector
        let query_vector: Vec<f32> = match vector_search.get_array("queryVector") {
            Ok(arr) => arr
                .iter()
                .filter_map(|v| match v {
                    Bson::Double(d) => Some(*d as f32),
                    Bson::Int32(i) => Some(*i as f32),
                    Bson::Int64(i) => Some(*i as f32),
                    _ => None,
                })
                .collect(),
            Err(_) => {
                return doc! {
                    "ok": 0.0,
                    "errmsg": "Missing or invalid queryVector",
                    "code": 2
                };
            }
        };

        debug!(
            "Vector search: path={}, numCandidates={}, limit={}, vector_dim={}",
            path,
            num_candidates,
            limit,
            query_vector.len()
        );

        // Perform vector search
        match self
            .storage
            .vector_search(collection, &query_vector, limit as usize)
            .await
        {
            Ok(results) => {
                let bson_docs: Vec<bson::Document> = results
                    .into_iter()
                    .map(|r| {
                        let mut doc = json_to_bson(&r.payload);
                        doc.insert("score", Bson::Double(r.score as f64));
                        doc
                    })
                    .collect();

                doc! {
                    "ok": 1.0,
                    "cursor": {
                        "id": 0_i64,
                        "ns": format!("{}.{}", db, collection),
                        "firstBatch": bson_docs
                    }
                }
            }
            Err(e) => {
                doc! {
                    "ok": 0.0,
                    "errmsg": e.to_string(),
                    "code": 1
                }
            }
        }
    }

    async fn handle_create_indexes(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("createIndexes").unwrap_or_default();
        let indexes = match cmd.get_array("indexes") {
            Ok(idx) => idx,
            Err(_) => {
                return doc! {
                    "ok": 0.0,
                    "errmsg": "Missing indexes",
                    "code": 2
                };
            }
        };

        info!(
            "Creating {} indexes on {}",
            indexes.len(),
            collection
        );

        // For vector indexes, extract dimensions and create vector collection
        for index in indexes {
            if let Bson::Document(idx_doc) = index {
                if let Ok(key) = idx_doc.get_document("key") {
                    // Check for vector index
                    for (field, value) in key {
                        if let Bson::String(idx_type) = value {
                            if idx_type == "vectorSearch" || idx_type == "vector" {
                                // Get dimensions from options
                                let dims = idx_doc
                                    .get_document("vectorSearchOptions")
                                    .ok()
                                    .and_then(|opts| opts.get_i32("numDimensions").ok())
                                    .unwrap_or(1536) as usize;

                                let _ = self
                                    .storage
                                    .create_vector_collection(collection, dims)
                                    .await;

                                info!(
                                    "Created vector index on {}.{} with {} dimensions",
                                    collection, field, dims
                                );
                            }
                        }
                    }
                }
            }
        }

        doc! {
            "ok": 1.0,
            "numIndexesBefore": 1_i32,
            "numIndexesAfter": 2_i32
        }
    }

    async fn handle_count(&self, cmd: &bson::Document) -> bson::Document {
        let collection = cmd.get_str("count").unwrap_or_default();

        match self.storage.count_documents(collection).await {
            Ok(n) => doc! {
                "ok": 1.0,
                "n": n as i64
            },
            Err(e) => doc! {
                "ok": 0.0,
                "errmsg": e.to_string(),
                "code": 1
            },
        }
    }

    async fn handle_get_last_error(&self) -> bson::Document {
        doc! {
            "ok": 1.0,
            "n": 0_i32,
            "err": Bson::Null
        }
    }

    async fn handle_build_info(&self) -> bson::Document {
        doc! {
            "ok": 1.0,
            "version": "6.0.0",
            "gitVersion": "unknown",
            "modules": [],
            "allocator": "system",
            "javascriptEngine": "none",
            "sysInfo": "LumaDB MongoDB Compatibility Layer",
            "versionArray": [6_i32, 0_i32, 0_i32, 0_i32],
            "bits": 64_i32,
            "debug": false,
            "maxBsonObjectSize": 16777216_i32,
            "storageEngines": ["lumadb"]
        }
    }

    async fn handle_sasl(&self, cmd: &bson::Document) -> bson::Document {
        // Simplified SASL - accept any auth
        if cmd.contains_key("saslStart") {
            doc! {
                "ok": 1.0,
                "conversationId": 1_i32,
                "done": false,
                "payload": bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: vec![]
                }
            }
        } else {
            doc! {
                "ok": 1.0,
                "conversationId": 1_i32,
                "done": true,
                "payload": bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: vec![]
                }
            }
        }
    }
}
