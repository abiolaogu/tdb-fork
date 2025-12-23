//! Server orchestration

use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{info, error};
use anyhow::Result;

use lumadb_common::config::Config;
use lumadb_api::{RestServer, GraphQLServer, GrpcServer};
use lumadb_protocol::kafka::KafkaServer;
use lumadb_streaming::StreamingEngine;
use lumadb_storage::StorageEngine;
use lumadb_query::QueryEngine;
use lumadb_raft::RaftEngine;
use lumadb_cluster::ClusterManager;
use lumadb_security::SecurityManager;

/// Main LumaDB server orchestrating all components
pub struct LumaServer {
    config: Config,

    // Core engines
    storage: Arc<StorageEngine>,
    streaming: Arc<StreamingEngine>,
    query: Arc<QueryEngine>,
    raft: Arc<RaftEngine>,
    #[allow(dead_code)]
    cluster: Arc<ClusterManager>,
    security: Arc<SecurityManager>,

    // API servers
    rest_server: Arc<RestServer>,
    graphql_server: GraphQLServer,
    grpc_server: GrpcServer,
    kafka_server: Arc<KafkaServer>,

    // State
    running: Arc<RwLock<bool>>,
}

impl LumaServer {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Initializing LumaDB components...");

        // Initialize security first
        let security = Arc::new(SecurityManager::new(&config.security).await?);

        // Initialize storage engine
        let storage = Arc::new(StorageEngine::new(&config.storage).await?);

        // Initialize Raft consensus
        let raft = Arc::new(RaftEngine::new(&config.raft, storage.clone()).await?);

        // Initialize cluster manager
        let cluster = Arc::new(ClusterManager::new(&config.cluster, raft.clone()).await?);

        // Initialize streaming engine (100x performance)
        let streaming = Arc::new(
            StreamingEngine::new(
                &config.streaming,
                storage.clone(),
                Arc::new(lumadb_streaming::RaftStub),
            ).await?
        );

        // Initialize query engine
        let query = Arc::new(
            QueryEngine::new(&config.query, storage.clone()).await?
        );

        // Initialize API servers
        let rest_server = Arc::new(RestServer::new(
            &config.api.rest,
            query.clone(),
            streaming.clone(),
            security.clone(),
        ).await?);

        let graphql_server = GraphQLServer::new(
            &config.api.graphql,
            query.clone(),
            streaming.clone(),
            security.clone(),
        ).await?;

        let grpc_server = GrpcServer::new(
            &config.api.grpc,
            query.clone(),
            streaming.clone(),
            security.clone(),
        ).await?;

        let kafka_server = Arc::new(KafkaServer::new(
            &config.kafka,
            streaming.clone(),
            security.clone(),
        ).await?);

        info!("LumaDB initialization complete");

        Ok(Self {
            config,
            storage,
            streaming,
            query,
            raft,
            cluster,
            security,
            rest_server,
            graphql_server,
            grpc_server,
            kafka_server,
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start all server components
    pub async fn run(&self) -> Result<()> {
        *self.running.write().await = true;

        info!("Starting LumaDB server on multiple interfaces...");

        // Start REST/GraphQL server in a dedicated thread (actix-web has its own runtime)
        let rest_server = self.rest_server.clone();
        let _rest_thread = std::thread::spawn(move || {
            let rt = actix_rt::Runtime::new().expect("Failed to create actix runtime");
            rt.block_on(async move {
                if let Err(e) = rest_server.run().await {
                    error!("REST server error: {}", e);
                }
            });
        });

        // GraphQL is served through the REST server, just initialize
        let graphql_server = self.graphql_server.clone();
        let graphql_handle = tokio::spawn(async move {
            if let Err(e) = graphql_server.run().await {
                error!("GraphQL server error: {}", e);
            }
        });

        // gRPC server
        let grpc_server = self.grpc_server.clone();
        let grpc_handle = tokio::spawn(async move {
            if let Err(e) = grpc_server.run().await {
                error!("gRPC server error: {}", e);
            }
        });

        // Kafka protocol server
        let kafka_server = self.kafka_server.clone();
        let kafka_handle = tokio::spawn(async move {
            if let Err(e) = kafka_server.run().await {
                error!("Kafka server error: {}", e);
            }
        });

        // Streaming engine
        let streaming = self.streaming.clone();
        let streaming_handle = tokio::spawn(async move {
            if let Err(e) = streaming.run().await {
                error!("Streaming engine error: {}", e);
            }
        });

        info!("╔══════════════════════════════════════════════════════════╗");
        info!("║              LumaDB Server Started                        ║");
        info!("╠══════════════════════════════════════════════════════════╣");
        info!("║  REST API:    http://0.0.0.0:{}                       ║", self.config.api.rest.port);
        info!("║  GraphQL:     http://0.0.0.0:{}                       ║", self.config.api.graphql.port);
        info!("║  gRPC:        http://0.0.0.0:{}                       ║", self.config.api.grpc.port);
        info!("║  Kafka:       0.0.0.0:{}                              ║", self.config.kafka.port);
        info!("║  Metrics:     http://0.0.0.0:{}/metrics              ║", self.config.api.rest.port);
        info!("╚══════════════════════════════════════════════════════════╝");

        // Wait for tokio-based servers (REST runs in its own thread)
        tokio::try_join!(
            graphql_handle,
            grpc_handle,
            kafka_handle,
            streaming_handle,
        )?;

        Ok(())
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        info!("Initiating graceful shutdown...");
        *self.running.write().await = false;

        // Shutdown in reverse order
        self.kafka_server.as_ref().shutdown().await?;
        self.grpc_server.shutdown().await?;
        self.graphql_server.shutdown().await?;
        self.rest_server.as_ref().shutdown().await?;
        self.streaming.shutdown().await?;
        self.raft.shutdown().await?;
        self.storage.shutdown().await?;

        info!("Shutdown complete");
        Ok(())
    }
}
