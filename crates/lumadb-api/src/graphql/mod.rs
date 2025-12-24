//! GraphQL API implementation

use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Object, Schema, SimpleObject, InputObject};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{web, HttpResponse};
use tracing::info;

use lumadb_common::config::GraphQLApiConfig;
use lumadb_common::error::Result;
use lumadb_common::types::TopicConfig;
use lumadb_query::QueryEngine;
use lumadb_streaming::StreamingEngine;
use lumadb_security::SecurityManager;

/// GraphQL server
#[derive(Clone)]
pub struct GraphQLServer {
    config: GraphQLApiConfig,
    schema: Schema<QueryRoot, MutationRoot, EmptySubscription>,
}

impl GraphQLServer {
    /// Create a new GraphQL server
    pub async fn new(
        config: &GraphQLApiConfig,
        query: Arc<QueryEngine>,
        streaming: Arc<StreamingEngine>,
        security: Arc<SecurityManager>,
    ) -> Result<Self> {
        let schema = Schema::build(QueryRoot, MutationRoot, EmptySubscription)
            .data(query)
            .data(streaming)
            .data(security)
            .finish();

        Ok(Self {
            config: config.clone(),
            schema,
        })
    }

    /// Get the schema for embedding in Actix
    pub fn schema(&self) -> Schema<QueryRoot, MutationRoot, EmptySubscription> {
        self.schema.clone()
    }

    /// Run the GraphQL server
    pub async fn run(&self) -> Result<()> {
        info!("GraphQL API available on port {}", self.config.port);
        // GraphQL is served through the main REST server
        Ok(())
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down GraphQL API server");
        Ok(())
    }
}

// ============================================================================
// GraphQL Types
// ============================================================================

/// GraphQL Query Root
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Health check
    async fn health(&self) -> HealthStatus {
        HealthStatus {
            status: "healthy".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// List all collections
    async fn collections(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Collection>> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let collections = query_engine
            .list_collections()
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(collections
            .into_iter()
            .map(|c| Collection {
                name: c.name,
                count: c.count as i64,
                size_bytes: c.size_bytes as i64,
            })
            .collect())
    }

    /// List all topics
    async fn topics(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Topic>> {
        let streaming = ctx.data::<Arc<StreamingEngine>>()?;
        let topics = streaming
            .list_topics()
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(topics
            .into_iter()
            .map(|t| Topic {
                name: t.name,
                partitions: t.partitions.len() as i32,
                is_internal: t.is_internal,
            })
            .collect())
    }

    /// Execute a SQL query
    async fn query(
        &self,
        ctx: &Context<'_>,
        query: String,
    ) -> async_graphql::Result<serde_json::Value> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let result = query_engine
            .execute(&query, &[])
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(serde_json::json!({
            "rows": result.rows(),
            "columns": result.columns(),
            "execution_time_ms": result.execution_time_ms,
            "cached": result.cached
        }))
    }

    /// Find documents in a collection
    async fn find(
        &self,
        ctx: &Context<'_>,
        collection: String,
        filter: Option<serde_json::Value>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<serde_json::Value>> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let docs = query_engine
            .find(&collection, filter.as_ref(), limit.map(|l| l as usize))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(docs)
    }
}

/// GraphQL Mutation Root
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Create a collection
    async fn create_collection(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> async_graphql::Result<Collection> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let meta = query_engine
            .create_collection(&name, None, None)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(Collection {
            name: meta.name,
            count: meta.count as i64,
            size_bytes: meta.size_bytes as i64,
        })
    }

    /// Create a topic
    async fn create_topic(
        &self,
        ctx: &Context<'_>,
        name: String,
        partitions: Option<i32>,
    ) -> async_graphql::Result<Topic> {
        let streaming = ctx.data::<Arc<StreamingEngine>>()?;

        let mut config = TopicConfig::new(
            name.clone(),
            partitions.unwrap_or(3) as u32,
            1, // replication_factor
        );
        config.retention_ms = Some(7 * 24 * 60 * 60 * 1000); // 7 days
        config.segment_bytes = Some(1024 * 1024 * 1024);      // 1 GB

        streaming
            .create_topic(config)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(Topic {
            name,
            partitions: partitions.unwrap_or(3),
            is_internal: false,
        })
    }

    /// Insert documents into a collection
    async fn insert(
        &self,
        ctx: &Context<'_>,
        collection: String,
        documents: Vec<serde_json::Value>,
    ) -> async_graphql::Result<InsertResult> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let result = query_engine
            .insert(&collection, &documents)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(InsertResult {
            inserted_count: result.inserted_count as i64,
            ids: result.ids,
        })
    }

    /// Delete documents from a collection
    async fn delete(
        &self,
        ctx: &Context<'_>,
        collection: String,
        filter: serde_json::Value,
    ) -> async_graphql::Result<DeleteResult> {
        let query_engine = ctx.data::<Arc<QueryEngine>>()?;
        let result = query_engine
            .delete(&collection, &filter)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(DeleteResult {
            deleted_count: result.deleted_count as i64,
        })
    }
}

// ============================================================================
// GraphQL Response Types
// ============================================================================

#[derive(SimpleObject)]
struct HealthStatus {
    status: String,
    version: String,
}

#[derive(SimpleObject)]
struct Collection {
    name: String,
    count: i64,
    size_bytes: i64,
}

#[derive(SimpleObject)]
struct Topic {
    name: String,
    partitions: i32,
    is_internal: bool,
}

#[derive(SimpleObject)]
struct InsertResult {
    inserted_count: i64,
    ids: Vec<String>,
}

#[derive(SimpleObject)]
struct DeleteResult {
    deleted_count: i64,
}
