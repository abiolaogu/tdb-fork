//! Query optimizer for SQL transformation

use tracing::debug;

/// Query optimizer for SQL transformations
pub struct QueryOptimizer {
    /// Enable query rewriting
    enable_rewrite: bool,
    /// Enable index hints
    enable_index_hints: bool,
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self {
            enable_rewrite: true,
            enable_index_hints: true,
        }
    }
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Optimize a SQL query
    pub fn optimize(&self, sql: &str) -> OptimizedQuery {
        let sql = sql.trim();
        let normalized = self.normalize(sql);
        let query_type = self.detect_type(&normalized);
        
        // Apply optimizations
        let mut optimized = normalized.clone();
        
        if self.enable_rewrite {
            optimized = self.rewrite_query(&optimized, &query_type);
        }

        debug!("Optimized query: {} -> {}", sql, optimized);

        OptimizedQuery {
            original: sql.to_string(),
            optimized,
            query_type: query_type.clone(),
            cacheable: self.is_cacheable(&query_type),
            estimated_cost: self.estimate_cost(&query_type),
        }
    }

    fn normalize(&self, sql: &str) -> String {
        // Normalize whitespace
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn detect_type(&self, sql: &str) -> QueryType {
        let upper = sql.to_uppercase();
        if upper.starts_with("SELECT") {
            QueryType::Select
        } else if upper.starts_with("INSERT") {
            QueryType::Insert
        } else if upper.starts_with("UPDATE") {
            QueryType::Update
        } else if upper.starts_with("DELETE") {
            QueryType::Delete
        } else if upper.starts_with("CREATE") {
            QueryType::Ddl
        } else if upper.starts_with("DROP") {
            QueryType::Ddl
        } else if upper.starts_with("ALTER") {
            QueryType::Ddl
        } else if upper.starts_with("BEGIN") || upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK") {
            QueryType::Transaction
        } else {
            QueryType::Other
        }
    }

    fn rewrite_query(&self, sql: &str, query_type: &QueryType) -> String {
        match query_type {
            QueryType::Select => self.optimize_select(sql),
            _ => sql.to_string(),
        }
    }

    fn optimize_select(&self, sql: &str) -> String {
        let mut optimized = sql.to_string();

        // Add LIMIT if missing and SELECT without aggregation
        if !optimized.to_uppercase().contains("LIMIT") 
            && !optimized.to_uppercase().contains("COUNT(")
            && !optimized.to_uppercase().contains("SUM(")
            && !optimized.to_uppercase().contains("AVG(") {
            // Don't modify - let caller decide on limits
        }

        // Replace SELECT * with explicit columns if table schema is known
        // (would need schema registry for this)

        optimized
    }

    fn is_cacheable(&self, query_type: &QueryType) -> bool {
        matches!(query_type, QueryType::Select)
    }

    fn estimate_cost(&self, query_type: &QueryType) -> u32 {
        match query_type {
            QueryType::Select => 10,
            QueryType::Insert => 20,
            QueryType::Update => 25,
            QueryType::Delete => 25,
            QueryType::Ddl => 100,
            QueryType::Transaction => 5,
            QueryType::Other => 50,
        }
    }
}

/// Query type classification
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Transaction,
    Other,
}

/// Optimized query result
#[derive(Debug)]
pub struct OptimizedQuery {
    pub original: String,
    pub optimized: String,
    pub query_type: QueryType,
    pub cacheable: bool,
    pub estimated_cost: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_type_detection() {
        let optimizer = QueryOptimizer::new();
        
        assert_eq!(optimizer.detect_type("SELECT * FROM users"), QueryType::Select);
        assert_eq!(optimizer.detect_type("INSERT INTO users VALUES (1)"), QueryType::Insert);
        assert_eq!(optimizer.detect_type("UPDATE users SET name = 'x'"), QueryType::Update);
        assert_eq!(optimizer.detect_type("DELETE FROM users"), QueryType::Delete);
        assert_eq!(optimizer.detect_type("CREATE TABLE test (id INT)"), QueryType::Ddl);
    }

    #[test]
    fn test_cacheable() {
        let optimizer = QueryOptimizer::new();
        
        let select = optimizer.optimize("SELECT * FROM users");
        assert!(select.cacheable);
        
        let insert = optimizer.optimize("INSERT INTO users VALUES (1)");
        assert!(!insert.cacheable);
    }
}
