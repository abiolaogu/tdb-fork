//! LumaDB API Layer
//!
//! Provides:
//! - REST API (Actix-Web)
//! - GraphQL API (async-graphql)
//! - gRPC API (Tonic)
//! - WebSocket for real-time updates

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod graphql;
pub mod grpc;
pub mod rest;
pub mod websocket;

pub use rest::RestServer;
pub use graphql::GraphQLServer;
pub use grpc::GrpcServer;
