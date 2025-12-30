//! RLS Policy evaluator

use std::sync::Arc;

use crate::context::{RlsContext, RlsFunctions};
use crate::policy::{PolicyStore, RlsPolicy};

/// Policy evaluator for RLS enforcement
pub struct PolicyEvaluator {
    store: Arc<PolicyStore>,
}

impl PolicyEvaluator {
    /// Create a new policy evaluator
    pub fn new(store: Arc<PolicyStore>) -> Self {
        Self { store }
    }

    /// Check if a table has RLS enabled
    pub fn is_rls_enabled(&self, schema: &str, table: &str) -> bool {
        self.store.is_rls_enabled(schema, table)
    }

    /// Get the combined USING expression for a query
    pub fn get_using_expression(
        &self,
        schema: &str,
        table: &str,
        command: &str,
        context: &RlsContext,
    ) -> Option<String> {
        // Service role bypasses RLS
        if context.bypasses_rls() {
            return None;
        }

        // Check if RLS is enabled
        if !self.is_rls_enabled(schema, table) {
            return None;
        }

        let policies = self
            .store
            .get_applicable_policies(schema, table, command, &context.role);

        if policies.is_empty() {
            // No policies = deny all access for authenticated users, allow for owners
            return Some("false".to_string());
        }

        // Separate permissive and restrictive policies
        let (permissive, restrictive): (Vec<_>, Vec<_>) =
            policies.into_iter().partition(|p| p.permissive);

        let funcs = RlsFunctions::new(context.clone());

        // Build combined expression
        // Permissive: OR together (user needs to pass at least one)
        // Restrictive: AND together (user needs to pass all)
        let mut conditions = Vec::new();

        // Process permissive policies (OR)
        if !permissive.is_empty() {
            let permissive_conditions: Vec<String> = permissive
                .iter()
                .filter_map(|p| p.using_expression.as_ref())
                .map(|expr| funcs.substitute(expr))
                .map(|expr| format!("({})", expr))
                .collect();

            if !permissive_conditions.is_empty() {
                conditions.push(format!("({})", permissive_conditions.join(" OR ")));
            }
        }

        // Process restrictive policies (AND each one)
        for policy in &restrictive {
            if let Some(expr) = &policy.using_expression {
                conditions.push(format!("({})", funcs.substitute(expr)));
            }
        }

        if conditions.is_empty() {
            None
        } else {
            Some(conditions.join(" AND "))
        }
    }

    /// Get the combined WITH CHECK expression for INSERT/UPDATE
    pub fn get_check_expression(
        &self,
        schema: &str,
        table: &str,
        command: &str,
        context: &RlsContext,
    ) -> Option<String> {
        // Service role bypasses RLS
        if context.bypasses_rls() {
            return None;
        }

        // Check if RLS is enabled
        if !self.is_rls_enabled(schema, table) {
            return None;
        }

        let policies = self
            .store
            .get_applicable_policies(schema, table, command, &context.role);

        if policies.is_empty() {
            return Some("false".to_string());
        }

        let funcs = RlsFunctions::new(context.clone());

        // All CHECK expressions must be satisfied (AND)
        let conditions: Vec<String> = policies
            .iter()
            .filter_map(|p| {
                // Use check_expression if available, otherwise using_expression
                p.check_expression.as_ref().or(p.using_expression.as_ref())
            })
            .map(|expr| funcs.substitute(expr))
            .map(|expr| format!("({})", expr))
            .collect();

        if conditions.is_empty() {
            None
        } else {
            Some(conditions.join(" AND "))
        }
    }

    /// Evaluate if a row passes RLS for the given context
    /// This is used for row-by-row evaluation (less efficient than query rewriting)
    pub fn evaluate_row(
        &self,
        schema: &str,
        table: &str,
        command: &str,
        context: &RlsContext,
        row: &serde_json::Value,
    ) -> bool {
        // Service role always passes
        if context.bypasses_rls() {
            return true;
        }

        // If RLS not enabled, allow
        if !self.is_rls_enabled(schema, table) {
            return true;
        }

        // Get the expression
        let expression = match command.to_uppercase().as_str() {
            "INSERT" | "UPDATE" => self.get_check_expression(schema, table, command, context),
            _ => self.get_using_expression(schema, table, command, context),
        };

        // No expression = check passed (policies might just set context)
        let Some(expr) = expression else {
            return true;
        };

        // For now, do simple evaluation of common patterns
        // Full SQL expression evaluation would require a proper SQL engine
        evaluate_simple_expression(&expr, row, context)
    }
}

/// Simple expression evaluator for common RLS patterns
/// This handles patterns like: column = 'value', column = auth.uid(), true, false
fn evaluate_simple_expression(expr: &str, row: &serde_json::Value, context: &RlsContext) -> bool {
    let expr = expr.trim();

    // Handle boolean literals
    if expr.eq_ignore_ascii_case("true") || expr == "(true)" {
        return true;
    }
    if expr.eq_ignore_ascii_case("false") || expr == "(false)" {
        return false;
    }

    // Handle simple equality: column = 'value'
    if let Some((left, right)) = expr.split_once('=') {
        let left = left.trim().trim_matches(|c| c == '(' || c == ')').trim();
        let right = right.trim().trim_matches(|c| c == '(' || c == ')').trim();

        // Get the column value from the row
        if let Some(row_value) = row.get(left) {
            // Compare with the right side
            let right_value = right.trim_matches('\'');

            return match row_value {
                serde_json::Value::String(s) => s == right_value,
                serde_json::Value::Number(n) => n.to_string() == right_value,
                serde_json::Value::Bool(b) => b.to_string() == right_value,
                serde_json::Value::Null => right.eq_ignore_ascii_case("NULL"),
                _ => false,
            };
        }
    }

    // Handle AND
    if let Some((left, right)) = expr.split_once(" AND ") {
        return evaluate_simple_expression(left, row, context)
            && evaluate_simple_expression(right, row, context);
    }

    // Handle OR
    if let Some((left, right)) = expr.split_once(" OR ") {
        return evaluate_simple_expression(left, row, context)
            || evaluate_simple_expression(right, row, context);
    }

    // Default: allow if we can't evaluate
    // In production, this should log a warning and probably deny
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyCommand;
    use uuid::Uuid;

    fn create_test_evaluator() -> (PolicyEvaluator, Arc<PolicyStore>) {
        let store = Arc::new(PolicyStore::new());
        let evaluator = PolicyEvaluator::new(store.clone());
        (evaluator, store)
    }

    #[test]
    fn test_no_rls_enabled() {
        let (evaluator, _store) = create_test_evaluator();
        let context = RlsContext::authenticated(Uuid::new_v4(), "authenticated");

        let expr = evaluator.get_using_expression("public", "users", "SELECT", &context);
        assert!(expr.is_none());
    }

    #[test]
    fn test_service_role_bypass() {
        let (evaluator, store) = create_test_evaluator();
        store.enable_rls("public", "users", false);

        let policy =
            RlsPolicy::new("deny_all", "public", "users", PolicyCommand::All).with_using("false");
        store.create_policy(policy).unwrap();

        let context = RlsContext::service_role();
        let expr = evaluator.get_using_expression("public", "users", "SELECT", &context);
        assert!(expr.is_none());
    }

    #[test]
    fn test_permissive_policy() {
        let (evaluator, store) = create_test_evaluator();
        store.enable_rls("public", "users", false);

        let user_id = Uuid::new_v4();
        let policy = RlsPolicy::new("own_rows", "public", "users", PolicyCommand::Select)
            .with_using("user_id = auth.uid()");
        store.create_policy(policy).unwrap();

        let context = RlsContext::authenticated(user_id, "authenticated");
        let expr = evaluator
            .get_using_expression("public", "users", "SELECT", &context)
            .unwrap();

        assert!(expr.contains(&user_id.to_string()));
    }

    #[test]
    fn test_evaluate_row() {
        let (evaluator, store) = create_test_evaluator();
        store.enable_rls("public", "posts", false);

        let user_id = Uuid::new_v4();
        let policy = RlsPolicy::new("own_posts", "public", "posts", PolicyCommand::Select)
            .with_using("author_id = auth.uid()");
        store.create_policy(policy).unwrap();

        let context = RlsContext::authenticated(user_id, "authenticated");

        // Row with matching author
        let row = serde_json::json!({
            "id": 1,
            "author_id": user_id.to_string(),
            "title": "My Post"
        });
        assert!(evaluator.evaluate_row("public", "posts", "SELECT", &context, &row));

        // Row with different author
        let other_row = serde_json::json!({
            "id": 2,
            "author_id": Uuid::new_v4().to_string(),
            "title": "Other Post"
        });
        assert!(!evaluator.evaluate_row("public", "posts", "SELECT", &context, &other_row));
    }
}
