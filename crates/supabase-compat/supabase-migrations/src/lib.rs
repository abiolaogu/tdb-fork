//! Migration Tools for Supabase Compatibility
//!
//! Provides database migration management:
//! - Schema versioning
//! - Migration tracking
//! - Up/down migrations

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Migration direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationDirection {
    Up,
    Down,
}

/// A database migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Migration version (timestamp-based)
    pub version: String,
    /// Migration name
    pub name: String,
    /// Up SQL
    pub up_sql: String,
    /// Down SQL
    pub down_sql: String,
}

impl Migration {
    pub fn new(version: &str, name: &str, up_sql: &str, down_sql: &str) -> Self {
        Self {
            version: version.to_string(),
            name: name.to_string(),
            up_sql: up_sql.to_string(),
            down_sql: down_sql.to_string(),
        }
    }
}

/// Applied migration record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: String,
    pub name: String,
    pub applied_at: DateTime<Utc>,
    pub checksum: String,
}

/// Migration result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResult {
    pub version: String,
    pub name: String,
    pub direction: String,
    pub success: bool,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Migration manager
pub struct MigrationManager {
    /// All registered migrations
    migrations: RwLock<BTreeMap<String, Migration>>,
    /// Applied migrations
    applied: Arc<RwLock<BTreeMap<String, AppliedMigration>>>,
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            migrations: RwLock::new(BTreeMap::new()),
            applied: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Register a migration
    pub fn register(&self, migration: Migration) {
        self.migrations
            .write()
            .insert(migration.version.clone(), migration);
    }

    /// Get pending migrations
    pub fn pending(&self) -> Vec<Migration> {
        let migrations = self.migrations.read();
        let applied = self.applied.read();

        migrations
            .values()
            .filter(|m| !applied.contains_key(&m.version))
            .cloned()
            .collect()
    }

    /// Get applied migrations
    pub fn applied(&self) -> Vec<AppliedMigration> {
        self.applied.read().values().cloned().collect()
    }

    /// Run pending migrations
    pub async fn migrate(&self) -> Vec<MigrationResult> {
        let pending = self.pending();
        let mut results = Vec::new();

        for migration in pending {
            let start = std::time::Instant::now();

            // In production, would execute SQL
            let result = MigrationResult {
                version: migration.version.clone(),
                name: migration.name.clone(),
                direction: "up".to_string(),
                success: true,
                duration_ms: start.elapsed().as_millis() as u64,
                error: None,
            };

            // Record as applied
            self.applied.write().insert(
                migration.version.clone(),
                AppliedMigration {
                    version: migration.version,
                    name: migration.name,
                    applied_at: Utc::now(),
                    checksum: "placeholder".to_string(),
                },
            );

            results.push(result);
        }

        results
    }

    /// Rollback last migration
    pub async fn rollback(&self) -> Option<MigrationResult> {
        let last = self.applied.read().keys().last().cloned();

        if let Some(version) = last {
            let migration = self.migrations.read().get(&version).cloned();

            if let Some(migration) = migration {
                let start = std::time::Instant::now();

                // In production, would execute down SQL
                self.applied.write().remove(&version);

                return Some(MigrationResult {
                    version: migration.version,
                    name: migration.name,
                    direction: "down".to_string(),
                    success: true,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: None,
                });
            }
        }

        None
    }

    /// Get migration status
    pub fn status(&self) -> MigrationStatus {
        let total = self.migrations.read().len();
        let applied = self.applied.read().len();

        MigrationStatus {
            total_migrations: total,
            applied_migrations: applied,
            pending_migrations: total - applied,
            last_applied: self.applied.read().values().last().cloned(),
        }
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration status summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    pub total_migrations: usize,
    pub applied_migrations: usize,
    pub pending_migrations: usize,
    pub last_applied: Option<AppliedMigration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_migration_manager() {
        let manager = MigrationManager::new();

        manager.register(Migration::new(
            "20240101000000",
            "create_users",
            "CREATE TABLE users (id uuid PRIMARY KEY)",
            "DROP TABLE users",
        ));

        assert_eq!(manager.pending().len(), 1);

        let results = manager.migrate().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].success);

        assert_eq!(manager.pending().len(), 0);
        assert_eq!(manager.applied().len(), 1);
    }
}
