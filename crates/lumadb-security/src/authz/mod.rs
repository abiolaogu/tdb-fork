//! Authorization (RBAC and ACLs)

use std::collections::HashMap;

use dashmap::DashMap;

/// Authorization manager
pub struct Authorizer {
    /// Role definitions
    roles: DashMap<String, Role>,
    /// Resource ACLs
    acls: DashMap<String, Vec<AclEntry>>,
}

/// Role definition
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

/// Permission
#[derive(Debug, Clone)]
pub struct Permission {
    pub resource: String,
    pub action: String,
}

/// ACL entry
#[derive(Debug, Clone)]
pub struct AclEntry {
    pub principal: String,
    pub permission: Permission,
    pub allow: bool,
}

impl Authorizer {
    /// Create a new authorizer with default roles
    pub fn new() -> Self {
        let auth = Self {
            roles: DashMap::new(),
            acls: DashMap::new(),
        };

        // Define default roles
        auth.roles.insert(
            "admin".to_string(),
            Role {
                name: "admin".to_string(),
                permissions: vec![Permission {
                    resource: "*".to_string(),
                    action: "*".to_string(),
                }],
            },
        );

        auth.roles.insert(
            "user".to_string(),
            Role {
                name: "user".to_string(),
                permissions: vec![
                    Permission {
                        resource: "topic:*".to_string(),
                        action: "read".to_string(),
                    },
                    Permission {
                        resource: "topic:*".to_string(),
                        action: "write".to_string(),
                    },
                    Permission {
                        resource: "collection:*".to_string(),
                        action: "read".to_string(),
                    },
                ],
            },
        );

        auth.roles.insert(
            "readonly".to_string(),
            Role {
                name: "readonly".to_string(),
                permissions: vec![Permission {
                    resource: "*".to_string(),
                    action: "read".to_string(),
                }],
            },
        );

        auth
    }

    /// Check if roles have permission
    pub fn check(&self, roles: &[String], permission: &Permission) -> bool {
        for role_name in roles {
            if let Some(role) = self.roles.get(role_name) {
                for perm in &role.permissions {
                    if self.matches_permission(perm, permission) {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn matches_permission(&self, have: &Permission, want: &Permission) -> bool {
        let resource_match = have.resource == "*"
            || have.resource == want.resource
            || (have.resource.ends_with(":*")
                && want.resource.starts_with(&have.resource[..have.resource.len() - 1]));

        let action_match = have.action == "*" || have.action == want.action;

        resource_match && action_match
    }

    /// Add a role
    pub fn add_role(&self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    /// Add an ACL entry
    pub fn add_acl(&self, resource: &str, entry: AclEntry) {
        self.acls
            .entry(resource.to_string())
            .or_insert_with(Vec::new)
            .push(entry);
    }
}

impl Default for Authorizer {
    fn default() -> Self {
        Self::new()
    }
}
