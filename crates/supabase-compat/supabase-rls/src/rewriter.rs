//! SQL query rewriter for RLS enforcement

use std::sync::Arc;

use crate::context::RlsContext;
use crate::evaluator::PolicyEvaluator;

/// Query rewriter that injects RLS conditions into SQL queries
pub struct QueryRewriter {
    evaluator: Arc<PolicyEvaluator>,
}

impl QueryRewriter {
    /// Create a new query rewriter
    pub fn new(evaluator: Arc<PolicyEvaluator>) -> Self {
        Self { evaluator }
    }

    /// Rewrite a SELECT query with RLS conditions
    pub fn rewrite_select(
        &self,
        sql: &str,
        schema: &str,
        table: &str,
        context: &RlsContext,
    ) -> String {
        if let Some(condition) = self
            .evaluator
            .get_using_expression(schema, table, "SELECT", context)
        {
            inject_where_condition(sql, &condition)
        } else {
            sql.to_string()
        }
    }

    /// Rewrite an UPDATE query with RLS conditions
    pub fn rewrite_update(
        &self,
        sql: &str,
        schema: &str,
        table: &str,
        context: &RlsContext,
    ) -> String {
        // For UPDATE, we need both USING (which rows can be updated)
        // and WITH CHECK (what values are allowed)
        if let Some(condition) = self
            .evaluator
            .get_using_expression(schema, table, "UPDATE", context)
        {
            inject_where_condition(sql, &condition)
        } else {
            sql.to_string()
        }
    }

    /// Rewrite a DELETE query with RLS conditions
    pub fn rewrite_delete(
        &self,
        sql: &str,
        schema: &str,
        table: &str,
        context: &RlsContext,
    ) -> String {
        if let Some(condition) = self
            .evaluator
            .get_using_expression(schema, table, "DELETE", context)
        {
            inject_where_condition(sql, &condition)
        } else {
            sql.to_string()
        }
    }

    /// Check if an INSERT is allowed by RLS
    pub fn check_insert(&self, schema: &str, table: &str, context: &RlsContext) -> Option<String> {
        self.evaluator
            .get_check_expression(schema, table, "INSERT", context)
    }

    /// Get the check expression for validating UPDATE values
    pub fn get_update_check(
        &self,
        schema: &str,
        table: &str,
        context: &RlsContext,
    ) -> Option<String> {
        self.evaluator
            .get_check_expression(schema, table, "UPDATE", context)
    }
}

/// Inject a WHERE condition into a SQL query
fn inject_where_condition(sql: &str, condition: &str) -> String {
    let sql_upper = sql.to_uppercase();

    // Find WHERE clause
    if let Some(where_pos) = sql_upper.find(" WHERE ") {
        // Insert condition after WHERE
        let (before, after) = sql.split_at(where_pos + 7);
        format!("{}({}) AND ({})", before, condition, after)
    } else if let Some(order_pos) = sql_upper.find(" ORDER BY ") {
        // No WHERE, but has ORDER BY - insert before ORDER BY
        let (before, after) = sql.split_at(order_pos);
        format!("{} WHERE {} {}", before, condition, after)
    } else if let Some(limit_pos) = sql_upper.find(" LIMIT ") {
        // No WHERE, no ORDER BY, but has LIMIT
        let (before, after) = sql.split_at(limit_pos);
        format!("{} WHERE {} {}", before, condition, after)
    } else if let Some(group_pos) = sql_upper.find(" GROUP BY ") {
        // No WHERE, but has GROUP BY
        let (before, after) = sql.split_at(group_pos);
        format!("{} WHERE {} {}", before, condition, after)
    } else {
        // No WHERE clause at all - append it
        format!("{} WHERE {}", sql.trim_end_matches(';'), condition)
    }
}

/// SQL command type detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCommand {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

impl SqlCommand {
    /// Detect the command type from SQL
    pub fn detect(sql: &str) -> Self {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("SELECT") {
            SqlCommand::Select
        } else if sql_upper.starts_with("INSERT") {
            SqlCommand::Insert
        } else if sql_upper.starts_with("UPDATE") {
            SqlCommand::Update
        } else if sql_upper.starts_with("DELETE") {
            SqlCommand::Delete
        } else {
            SqlCommand::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_where_simple() {
        let sql = "SELECT * FROM users";
        let result = inject_where_condition(sql, "user_id = '123'");
        assert_eq!(result, "SELECT * FROM users WHERE user_id = '123'");
    }

    #[test]
    fn test_inject_where_existing() {
        let sql = "SELECT * FROM users WHERE active = true";
        let result = inject_where_condition(sql, "user_id = '123'");
        assert!(result.contains("(user_id = '123')"));
        assert!(result.contains("(active = true)"));
    }

    #[test]
    fn test_inject_where_with_order() {
        let sql = "SELECT * FROM users ORDER BY created_at";
        let result = inject_where_condition(sql, "user_id = '123'");
        assert!(result.contains("WHERE user_id = '123'"));
        assert!(result.contains("ORDER BY created_at"));
    }

    #[test]
    fn test_sql_command_detection() {
        assert_eq!(
            SqlCommand::detect("SELECT * FROM users"),
            SqlCommand::Select
        );
        assert_eq!(
            SqlCommand::detect("INSERT INTO users VALUES (1)"),
            SqlCommand::Insert
        );
        assert_eq!(
            SqlCommand::detect("UPDATE users SET name = 'x'"),
            SqlCommand::Update
        );
        assert_eq!(SqlCommand::detect("DELETE FROM users"), SqlCommand::Delete);
    }
}
