//! RLS Policy definitions and storage

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// RLS Policy command types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

impl PolicyCommand {
    /// Check if this command applies to a given operation
    pub fn applies_to(&self, operation: &str) -> bool {
        match self {
            PolicyCommand::All => true,
            PolicyCommand::Select => operation.eq_ignore_ascii_case("SELECT"),
            PolicyCommand::Insert => operation.eq_ignore_ascii_case("INSERT"),
            PolicyCommand::Update => operation.eq_ignore_ascii_case("UPDATE"),
            PolicyCommand::Delete => operation.eq_ignore_ascii_case("DELETE"),
        }
    }
}

impl Default for PolicyCommand {
    fn default() -> Self {
        PolicyCommand::All
    }
}

/// RLS Policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    /// Unique policy ID
    pub id: Uuid,
    /// Policy name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Command this policy applies to
    pub command: PolicyCommand,
    /// Roles this policy applies to (empty = all roles)
    pub roles: Vec<String>,
    /// USING expression (for SELECT, UPDATE, DELETE)
    /// Evaluated to determine which rows are visible
    pub using_expression: Option<String>,
    /// WITH CHECK expression (for INSERT, UPDATE)
    /// Evaluated to determine if new/modified rows are allowed
    pub check_expression: Option<String>,
    /// Whether the policy is permissive or restrictive
    pub permissive: bool,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

impl RlsPolicy {
    /// Create a new RLS policy
    pub fn new(name: &str, schema: &str, table: &str, command: PolicyCommand) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            command,
            roles: vec![],
            using_expression: None,
            check_expression: None,
            permissive: true,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set USING expression
    pub fn with_using(mut self, expr: &str) -> Self {
        self.using_expression = Some(expr.to_string());
        self
    }

    /// Set WITH CHECK expression
    pub fn with_check(mut self, expr: &str) -> Self {
        self.check_expression = Some(expr.to_string());
        self
    }

    /// Set roles
    pub fn for_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    /// Make policy restrictive
    pub fn restrictive(mut self) -> Self {
        self.permissive = false;
        self
    }

    /// Check if policy applies to a given role
    pub fn applies_to_role(&self, role: &str) -> bool {
        self.roles.is_empty() || self.roles.iter().any(|r| r == role)
    }

    /// Get the qualified table name
    pub fn qualified_table(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// Table RLS settings
#[derive(Debug, Clone, Default)]
pub struct TableRlsSettings {
    /// Whether RLS is enabled for this table
    pub enabled: bool,
    /// Whether to force RLS even for table owners
    pub force: bool,
}

/// Policy store for managing RLS policies
pub struct PolicyStore {
    /// Policies by table (schema.table -> policies)
    policies: Arc<RwLock<HashMap<String, Vec<RlsPolicy>>>>,
    /// Table RLS settings
    table_settings: Arc<RwLock<HashMap<String, TableRlsSettings>>>,
}

impl PolicyStore {
    /// Create a new policy store
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            table_settings: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Enable RLS on a table
    pub fn enable_rls(&self, schema: &str, table: &str, force: bool) {
        let key = format!("{}.{}", schema, table);
        self.table_settings.write().insert(
            key,
            TableRlsSettings {
                enabled: true,
                force,
            },
        );
    }

    /// Disable RLS on a table
    pub fn disable_rls(&self, schema: &str, table: &str) {
        let key = format!("{}.{}", schema, table);
        self.table_settings.write().remove(&key);
    }

    /// Check if RLS is enabled for a table
    pub fn is_rls_enabled(&self, schema: &str, table: &str) -> bool {
        let key = format!("{}.{}", schema, table);
        self.table_settings
            .read()
            .get(&key)
            .map(|s| s.enabled)
            .unwrap_or(false)
    }

    /// Create a new policy
    pub fn create_policy(&self, policy: RlsPolicy) -> Result<(), String> {
        let key = policy.qualified_table();
        let mut policies = self.policies.write();

        let table_policies = policies.entry(key).or_insert_with(Vec::new);

        // Check for duplicate name
        if table_policies.iter().any(|p| p.name == policy.name) {
            return Err(format!("Policy '{}' already exists", policy.name));
        }

        table_policies.push(policy);
        Ok(())
    }

    /// Drop a policy
    pub fn drop_policy(&self, schema: &str, table: &str, name: &str) -> Result<(), String> {
        let key = format!("{}.{}", schema, table);
        let mut policies = self.policies.write();

        if let Some(table_policies) = policies.get_mut(&key) {
            let original_len = table_policies.len();
            table_policies.retain(|p| p.name != name);

            if table_policies.len() == original_len {
                return Err(format!("Policy '{}' not found", name));
            }
        } else {
            return Err(format!("No policies found for {}.{}", schema, table));
        }

        Ok(())
    }

    /// Get all policies for a table
    pub fn get_policies(&self, schema: &str, table: &str) -> Vec<RlsPolicy> {
        let key = format!("{}.{}", schema, table);
        self.policies.read().get(&key).cloned().unwrap_or_default()
    }

    /// Get applicable policies for a table, command, and role
    pub fn get_applicable_policies(
        &self,
        schema: &str,
        table: &str,
        command: &str,
        role: &str,
    ) -> Vec<RlsPolicy> {
        self.get_policies(schema, table)
            .into_iter()
            .filter(|p| p.command.applies_to(command) && p.applies_to_role(role))
            .collect()
    }
}

impl Default for PolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = RlsPolicy::new("users_policy", "public", "users", PolicyCommand::Select)
            .with_using("auth.uid() = user_id");

        assert_eq!(policy.name, "users_policy");
        assert_eq!(
            policy.using_expression,
            Some("auth.uid() = user_id".to_string())
        );
    }

    #[test]
    fn test_policy_store() {
        let store = PolicyStore::new();

        let policy =
            RlsPolicy::new("test_policy", "public", "users", PolicyCommand::All).with_using("true");

        store.create_policy(policy).unwrap();

        let policies = store.get_policies("public", "users");
        assert_eq!(policies.len(), 1);
    }

    #[test]
    fn test_enable_rls() {
        let store = PolicyStore::new();

        assert!(!store.is_rls_enabled("public", "users"));

        store.enable_rls("public", "users", false);

        assert!(store.is_rls_enabled("public", "users"));
    }

    #[test]
    fn test_command_applies() {
        assert!(PolicyCommand::All.applies_to("SELECT"));
        assert!(PolicyCommand::All.applies_to("INSERT"));
        assert!(PolicyCommand::Select.applies_to("SELECT"));
        assert!(!PolicyCommand::Select.applies_to("INSERT"));
    }
}
