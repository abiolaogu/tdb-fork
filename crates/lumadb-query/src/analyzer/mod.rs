//! Semantic analyzer for queries

use std::sync::Arc;

use lumadb_common::error::{Result, Error, QueryError};
use lumadb_storage::StorageEngine;

use crate::parser::Ast;

/// Semantic analyzer
pub struct Analyzer {
    storage: Arc<StorageEngine>,
}

impl Analyzer {
    /// Create a new analyzer
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    /// Analyze an AST
    pub fn analyze(&self, ast: &Ast) -> Result<AnalyzedQuery> {
        match ast {
            Ast::Select { columns, from, filter, order_by, limit } => {
                self.analyze_select(columns, from, filter, order_by, limit)
            }
            Ast::Insert { table, columns, values } => {
                self.analyze_insert(table, columns, values)
            }
            Ast::Update { table, set, filter } => {
                self.analyze_update(table, set, filter)
            }
            Ast::Delete { table, filter } => {
                self.analyze_delete(table, filter)
            }
            Ast::CreateTable { name, columns } => {
                Ok(AnalyzedQuery::CreateTable {
                    name: name.clone(),
                    columns: columns.clone(),
                })
            }
            Ast::DropTable { name } => {
                Ok(AnalyzedQuery::DropTable { name: name.clone() })
            }
            Ast::Stream { topic, filter, limit } => {
                Ok(AnalyzedQuery::Stream {
                    topic: topic.clone(),
                    filter: filter.clone(),
                    limit: *limit,
                })
            }
            Ast::TopicCreate { name, partitions, replication } => {
                Ok(AnalyzedQuery::TopicCreate {
                    name: name.clone(),
                    partitions: *partitions,
                    replication: *replication,
                })
            }
            Ast::TopicList => Ok(AnalyzedQuery::TopicList),
            Ast::TopicDelete { name } => {
                Ok(AnalyzedQuery::TopicDelete { name: name.clone() })
            }
            Ast::VectorSearch { collection, vector, k } => {
                Ok(AnalyzedQuery::VectorSearch {
                    collection: collection.clone(),
                    vector: vector.clone(),
                    k: *k,
                })
            }
        }
    }

    fn analyze_select(
        &self,
        columns: &[String],
        from: &str,
        filter: &Option<crate::parser::Expr>,
        order_by: &Option<Vec<(String, bool)>>,
        limit: &Option<usize>,
    ) -> Result<AnalyzedQuery> {
        // TODO: Validate table exists and columns are valid

        Ok(AnalyzedQuery::Select {
            columns: columns.to_vec(),
            from: from.to_string(),
            filter: filter.clone(),
            order_by: order_by.clone(),
            limit: *limit,
        })
    }

    fn analyze_insert(
        &self,
        table: &str,
        columns: &[String],
        values: &[Vec<serde_json::Value>],
    ) -> Result<AnalyzedQuery> {
        Ok(AnalyzedQuery::Insert {
            table: table.to_string(),
            columns: columns.to_vec(),
            values: values.to_vec(),
        })
    }

    fn analyze_update(
        &self,
        table: &str,
        set: &std::collections::HashMap<String, serde_json::Value>,
        filter: &Option<crate::parser::Expr>,
    ) -> Result<AnalyzedQuery> {
        Ok(AnalyzedQuery::Update {
            table: table.to_string(),
            set: set.clone(),
            filter: filter.clone(),
        })
    }

    fn analyze_delete(
        &self,
        table: &str,
        filter: &Option<crate::parser::Expr>,
    ) -> Result<AnalyzedQuery> {
        Ok(AnalyzedQuery::Delete {
            table: table.to_string(),
            filter: filter.clone(),
        })
    }
}

/// Analyzed query with type information
#[derive(Debug, Clone)]
pub enum AnalyzedQuery {
    Select {
        columns: Vec<String>,
        from: String,
        filter: Option<crate::parser::Expr>,
        order_by: Option<Vec<(String, bool)>>,
        limit: Option<usize>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<serde_json::Value>>,
    },
    Update {
        table: String,
        set: std::collections::HashMap<String, serde_json::Value>,
        filter: Option<crate::parser::Expr>,
    },
    Delete {
        table: String,
        filter: Option<crate::parser::Expr>,
    },
    CreateTable {
        name: String,
        columns: Vec<crate::parser::ColumnDef>,
    },
    DropTable {
        name: String,
    },
    Stream {
        topic: String,
        filter: Option<crate::parser::Expr>,
        limit: Option<usize>,
    },
    TopicCreate {
        name: String,
        partitions: u32,
        replication: u32,
    },
    TopicList,
    TopicDelete {
        name: String,
    },
    VectorSearch {
        collection: String,
        vector: Vec<f32>,
        k: usize,
    },
}
