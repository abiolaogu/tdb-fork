//! Admin Dashboard API for Supabase Compatibility
//!
//! Provides management APIs for:
//! - Project management
//! - User administration
//! - Service configuration
//! - Logs and analytics

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Project information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub organization_id: String,
    pub region: String,
    pub status: ProjectStatus,
    pub config: ProjectConfig,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectStatus {
    Active,
    Paused,
    Deleted,
}

/// Project configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub db_schema: String,
    pub jwt_secret: String,
    pub anon_key: String,
    pub service_role_key: String,
}

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: String,
    pub project_id: String,
    pub level: LogLevel,
    pub service: String,
    pub message: String,
    pub metadata: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Usage stats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageStats {
    pub project_id: String,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub database_size_bytes: u64,
    pub storage_size_bytes: u64,
    pub bandwidth_bytes: u64,
    pub function_invocations: u64,
    pub realtime_connections: u64,
    pub auth_users: u64,
}

/// Admin API
pub struct AdminApi {
    projects: Arc<RwLock<HashMap<String, Project>>>,
    logs: Arc<RwLock<Vec<LogEntry>>>,
}

impl AdminApi {
    pub fn new() -> Self {
        Self {
            projects: Arc::new(RwLock::new(HashMap::new())),
            logs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a new project
    pub fn create_project(&self, name: &str, organization_id: &str, region: &str) -> Project {
        let project = Project {
            id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            organization_id: organization_id.to_string(),
            region: region.to_string(),
            status: ProjectStatus::Active,
            config: ProjectConfig {
                db_schema: "public".to_string(),
                jwt_secret: generate_secret(),
                anon_key: generate_api_key("anon"),
                service_role_key: generate_api_key("service"),
            },
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.projects
            .write()
            .insert(project.id.clone(), project.clone());
        project
    }

    /// Get a project
    pub fn get_project(&self, id: &str) -> Option<Project> {
        self.projects.read().get(id).cloned()
    }

    /// List projects
    pub fn list_projects(&self, organization_id: &str) -> Vec<Project> {
        self.projects
            .read()
            .values()
            .filter(|p| p.organization_id == organization_id)
            .cloned()
            .collect()
    }

    /// Pause a project
    pub fn pause_project(&self, id: &str) -> Option<Project> {
        let mut projects = self.projects.write();
        if let Some(project) = projects.get_mut(id) {
            project.status = ProjectStatus::Paused;
            project.updated_at = Utc::now();
            return Some(project.clone());
        }
        None
    }

    /// Resume a project
    pub fn resume_project(&self, id: &str) -> Option<Project> {
        let mut projects = self.projects.write();
        if let Some(project) = projects.get_mut(id) {
            project.status = ProjectStatus::Active;
            project.updated_at = Utc::now();
            return Some(project.clone());
        }
        None
    }

    /// Add a log entry
    pub fn log(&self, project_id: &str, level: LogLevel, service: &str, message: &str) {
        let entry = LogEntry {
            id: Uuid::new_v4().to_string(),
            project_id: project_id.to_string(),
            level,
            service: service.to_string(),
            message: message.to_string(),
            metadata: serde_json::Value::Null,
            timestamp: Utc::now(),
        };
        self.logs.write().push(entry);
    }

    /// Get logs for a project
    pub fn get_logs(&self, project_id: &str, limit: usize) -> Vec<LogEntry> {
        self.logs
            .read()
            .iter()
            .rev()
            .filter(|l| l.project_id == project_id)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get usage stats (mock)
    pub fn get_usage(&self, project_id: &str) -> UsageStats {
        let now = Utc::now();
        UsageStats {
            project_id: project_id.to_string(),
            period_start: now - chrono::Duration::days(30),
            period_end: now,
            database_size_bytes: 1024 * 1024 * 100, // 100MB
            storage_size_bytes: 1024 * 1024 * 500,  // 500MB
            bandwidth_bytes: 1024 * 1024 * 1024,    // 1GB
            function_invocations: 10000,
            realtime_connections: 500,
            auth_users: 1000,
        }
    }
}

impl Default for AdminApi {
    fn default() -> Self {
        Self::new()
    }
}

fn generate_secret() -> String {
    use base64::Engine;
    let bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn generate_api_key(role: &str) -> String {
    format!("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.placeholder_{}", role)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_project_creation() {
        let api = AdminApi::new();
        let project = api.create_project("my-project", "org1", "us-east-1");

        assert_eq!(project.name, "my-project");
        assert_eq!(project.status, ProjectStatus::Active);
    }

    #[test]
    fn test_project_pause_resume() {
        let api = AdminApi::new();
        let project = api.create_project("test", "org1", "us-east-1");

        api.pause_project(&project.id);
        let paused = api.get_project(&project.id).unwrap();
        assert_eq!(paused.status, ProjectStatus::Paused);

        api.resume_project(&project.id);
        let resumed = api.get_project(&project.id).unwrap();
        assert_eq!(resumed.status, ProjectStatus::Active);
    }
}
