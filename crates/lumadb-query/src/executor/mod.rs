//! Query executor with vectorized execution

use std::collections::HashMap;
use std::sync::Arc;

use lumadb_common::error::{Result, Error, QueryError};
use lumadb_storage::StorageEngine;

use crate::optimizer::PhysicalPlan;
use crate::QueryResult;

/// Query executor
pub struct Executor {
    storage: Arc<StorageEngine>,
}

impl Executor {
    /// Create a new executor
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    /// Execute a physical plan
    pub async fn execute(
        &self,
        plan: &PhysicalPlan,
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        match plan {
            PhysicalPlan::Scan { table, columns, .. } => {
                self.execute_scan(table, columns).await
            }
            PhysicalPlan::Filter { input, predicate, .. } => {
                self.execute_filter(input, predicate, params).await
            }
            PhysicalPlan::Sort { input, order_by, .. } => {
                self.execute_sort(input, order_by, params).await
            }
            PhysicalPlan::Limit { input, limit, .. } => {
                self.execute_limit(input, *limit, params).await
            }
            PhysicalPlan::Insert { table, columns, values, .. } => {
                self.execute_insert(table, columns, values).await
            }
            PhysicalPlan::Update { table, set, filter, .. } => {
                self.execute_update(table, set, filter).await
            }
            PhysicalPlan::Delete { table, filter, .. } => {
                self.execute_delete(table, filter).await
            }
            PhysicalPlan::CreateTable { name, columns } => {
                self.execute_create_table(name, columns).await
            }
            PhysicalPlan::DropTable { name } => {
                self.execute_drop_table(name).await
            }
            PhysicalPlan::TopicList => {
                self.execute_topic_list().await
            }
            PhysicalPlan::VectorSearch { collection, vector, k } => {
                self.execute_vector_search(collection, vector, *k).await
            }
            _ => Err(Error::Query(QueryError::ExecutionError(
                "Unsupported operation".to_string(),
            ))),
        }
    }

    async fn execute_scan(
        &self,
        table: &str,
        columns: &[String],
    ) -> Result<QueryResult> {
        let docs = self.storage.scan_documents(table, None, Some(1000)).await?;

        let rows: Vec<HashMap<String, serde_json::Value>> = docs
            .into_iter()
            .map(|doc| {
                let mut row = HashMap::new();
                row.insert("_id".to_string(), serde_json::Value::String(doc.id));

                if let serde_json::Value::Object(map) = doc.data {
                    for (k, v) in map {
                        if columns.contains(&"*".to_string()) || columns.contains(&k) {
                            row.insert(k, v);
                        }
                    }
                }

                row
            })
            .collect();

        Ok(QueryResult::new(rows))
    }

    async fn execute_filter(
        &self,
        input: &PhysicalPlan,
        predicate: &crate::parser::Expr,
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        let result = Box::pin(self.execute(input, params)).await?;

        // Apply filter to rows
        let filtered: Vec<_> = result
            .rows()
            .iter()
            .filter(|row| self.evaluate_predicate(predicate, row))
            .cloned()
            .collect();

        Ok(QueryResult::new(filtered))
    }

    async fn execute_sort(
        &self,
        input: &PhysicalPlan,
        order_by: &[(String, bool)],
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        let result = Box::pin(self.execute(input, params)).await?;
        let mut rows: Vec<_> = result.rows().to_vec();

        // Sort rows
        rows.sort_by(|a, b| {
            for (column, asc) in order_by {
                let va = a.get(column);
                let vb = b.get(column);

                let cmp = match (va, vb) {
                    (Some(va), Some(vb)) => {
                        let ord = va.to_string().cmp(&vb.to_string());
                        if *asc { ord } else { ord.reverse() }
                    }
                    _ => std::cmp::Ordering::Equal,
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(QueryResult::new(rows))
    }

    async fn execute_limit(
        &self,
        input: &PhysicalPlan,
        limit: usize,
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        let result = Box::pin(self.execute(input, params)).await?;
        let rows: Vec<_> = result.rows().iter().take(limit).cloned().collect();
        Ok(QueryResult::new(rows))
    }

    async fn execute_insert(
        &self,
        table: &str,
        columns: &[String],
        values: &[Vec<serde_json::Value>],
    ) -> Result<QueryResult> {
        for row_values in values {
            let mut data = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = row_values.get(i) {
                    data.insert(col.clone(), val.clone());
                }
            }

            let doc = lumadb_common::types::Document::new(serde_json::Value::Object(data));
            self.storage.insert_document(table, &doc).await?;
        }

        let mut result = HashMap::new();
        result.insert(
            "inserted".to_string(),
            serde_json::Value::Number(values.len().into()),
        );

        Ok(QueryResult::new(vec![result]))
    }

    async fn execute_update(
        &self,
        table: &str,
        set: &HashMap<String, serde_json::Value>,
        filter: &Option<crate::parser::Expr>,
    ) -> Result<QueryResult> {
        let docs = self.storage.scan_documents(table, None, None).await?;
        let mut updated = 0;

        for doc in docs {
            // Check filter
            let row: HashMap<String, serde_json::Value> = {
                let mut map = HashMap::new();
                if let serde_json::Value::Object(data) = &doc.data {
                    for (k, v) in data {
                        map.insert(k.clone(), v.clone());
                    }
                }
                map
            };

            if let Some(pred) = filter {
                if !self.evaluate_predicate(pred, &row) {
                    continue;
                }
            }

            // Apply update
            let mut new_data = doc.data.clone();
            if let serde_json::Value::Object(ref mut map) = new_data {
                for (k, v) in set {
                    map.insert(k.clone(), v.clone());
                }
            }

            let updated_doc = lumadb_common::types::Document::with_id(&doc.id, new_data);
            self.storage.insert_document(table, &updated_doc).await?;
            updated += 1;
        }

        let mut result = HashMap::new();
        result.insert(
            "updated".to_string(),
            serde_json::Value::Number(updated.into()),
        );

        Ok(QueryResult::new(vec![result]))
    }

    async fn execute_delete(
        &self,
        table: &str,
        filter: &Option<crate::parser::Expr>,
    ) -> Result<QueryResult> {
        let docs = self.storage.scan_documents(table, None, None).await?;
        let mut deleted = 0;

        for doc in docs {
            let row: HashMap<String, serde_json::Value> = {
                let mut map = HashMap::new();
                if let serde_json::Value::Object(data) = &doc.data {
                    for (k, v) in data {
                        map.insert(k.clone(), v.clone());
                    }
                }
                map
            };

            if let Some(pred) = filter {
                if !self.evaluate_predicate(pred, &row) {
                    continue;
                }
            }

            self.storage.delete_document(table, &doc.id).await?;
            deleted += 1;
        }

        let mut result = HashMap::new();
        result.insert(
            "deleted".to_string(),
            serde_json::Value::Number(deleted.into()),
        );

        Ok(QueryResult::new(vec![result]))
    }

    async fn execute_create_table(
        &self,
        name: &str,
        _columns: &[crate::parser::ColumnDef],
    ) -> Result<QueryResult> {
        self.storage.create_collection(name).await?;

        let mut result = HashMap::new();
        result.insert("created".to_string(), serde_json::Value::String(name.to_string()));

        Ok(QueryResult::new(vec![result]))
    }

    async fn execute_drop_table(&self, name: &str) -> Result<QueryResult> {
        self.storage.delete_collection(name).await?;

        let mut result = HashMap::new();
        result.insert("dropped".to_string(), serde_json::Value::String(name.to_string()));

        Ok(QueryResult::new(vec![result]))
    }

    async fn execute_topic_list(&self) -> Result<QueryResult> {
        // This would integrate with the streaming engine
        Ok(QueryResult::new(vec![]))
    }

    async fn execute_vector_search(
        &self,
        collection: &str,
        vector: &[f32],
        k: usize,
    ) -> Result<QueryResult> {
        let index = self.storage.get_or_create_vector_index(collection, vector.len());
        let results = index.search(vector, k);

        let rows: Vec<HashMap<String, serde_json::Value>> = results
            .into_iter()
            .map(|(id, score)| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), serde_json::Value::String(id));
                row.insert("score".to_string(), serde_json::json!(score));
                row
            })
            .collect();

        Ok(QueryResult::new(rows))
    }

    fn evaluate_predicate(
        &self,
        predicate: &crate::parser::Expr,
        row: &HashMap<String, serde_json::Value>,
    ) -> bool {
        use crate::parser::{Expr, BinaryOperator};

        match predicate {
            Expr::Column(name) => {
                row.get(name)
                    .map(|v| !v.is_null())
                    .unwrap_or(false)
            }
            Expr::Literal(value) => {
                !value.is_null()
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left, row);
                let right_val = self.evaluate_expr(right, row);

                match op {
                    BinaryOperator::Eq => left_val == right_val,
                    BinaryOperator::Ne => left_val != right_val,
                    BinaryOperator::Lt => {
                        self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Less
                    }
                    BinaryOperator::Le => {
                        matches!(
                            self.compare_values(&left_val, &right_val),
                            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                        )
                    }
                    BinaryOperator::Gt => {
                        self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Greater
                    }
                    BinaryOperator::Ge => {
                        matches!(
                            self.compare_values(&left_val, &right_val),
                            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                        )
                    }
                    BinaryOperator::And => {
                        self.evaluate_predicate(left, row) && self.evaluate_predicate(right, row)
                    }
                    BinaryOperator::Or => {
                        self.evaluate_predicate(left, row) || self.evaluate_predicate(right, row)
                    }
                    _ => true,
                }
            }
            Expr::Function { name, args } => {
                // Handle function evaluation
                true
            }
        }
    }

    fn evaluate_expr(
        &self,
        expr: &crate::parser::Expr,
        row: &HashMap<String, serde_json::Value>,
    ) -> serde_json::Value {
        use crate::parser::Expr;

        match expr {
            Expr::Column(name) => row.get(name).cloned().unwrap_or(serde_json::Value::Null),
            Expr::Literal(value) => value.clone(),
            _ => serde_json::Value::Null,
        }
    }

    fn compare_values(
        &self,
        a: &serde_json::Value,
        b: &serde_json::Value,
    ) -> std::cmp::Ordering {
        match (a, b) {
            (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
                let a = a.as_f64().unwrap_or(0.0);
                let b = b.as_f64().unwrap_or(0.0);
                a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (serde_json::Value::String(a), serde_json::Value::String(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}
