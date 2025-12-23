//! Cost-based query optimizer

use lumadb_common::error::Result;
use lumadb_common::types::QueryPlan;

use crate::analyzer::AnalyzedQuery;

/// Query optimizer
pub struct Optimizer {
    /// Enable cost-based optimization
    cost_based: bool,
}

impl Optimizer {
    /// Create a new optimizer
    pub fn new() -> Self {
        Self { cost_based: true }
    }

    /// Optimize an analyzed query
    pub fn optimize(&self, query: &AnalyzedQuery) -> Result<PhysicalPlan> {
        match query {
            AnalyzedQuery::Select { columns, from, filter, order_by, limit } => {
                self.optimize_select(columns, from, filter, order_by, limit)
            }
            AnalyzedQuery::Insert { table, columns, values } => {
                Ok(PhysicalPlan::Insert {
                    table: table.clone(),
                    columns: columns.clone(),
                    values: values.clone(),
                    estimated_cost: values.len() as f64 * 10.0,
                })
            }
            AnalyzedQuery::Update { table, set, filter } => {
                Ok(PhysicalPlan::Update {
                    table: table.clone(),
                    set: set.clone(),
                    filter: filter.clone(),
                    estimated_cost: 100.0,
                })
            }
            AnalyzedQuery::Delete { table, filter } => {
                Ok(PhysicalPlan::Delete {
                    table: table.clone(),
                    filter: filter.clone(),
                    estimated_cost: 50.0,
                })
            }
            AnalyzedQuery::CreateTable { name, columns } => {
                Ok(PhysicalPlan::CreateTable {
                    name: name.clone(),
                    columns: columns.clone(),
                })
            }
            AnalyzedQuery::DropTable { name } => {
                Ok(PhysicalPlan::DropTable { name: name.clone() })
            }
            AnalyzedQuery::Stream { topic, filter, limit } => {
                Ok(PhysicalPlan::Stream {
                    topic: topic.clone(),
                    filter: filter.clone(),
                    limit: *limit,
                })
            }
            AnalyzedQuery::TopicCreate { name, partitions, replication } => {
                Ok(PhysicalPlan::TopicCreate {
                    name: name.clone(),
                    partitions: *partitions,
                    replication: *replication,
                })
            }
            AnalyzedQuery::TopicList => Ok(PhysicalPlan::TopicList),
            AnalyzedQuery::TopicDelete { name } => {
                Ok(PhysicalPlan::TopicDelete { name: name.clone() })
            }
            AnalyzedQuery::VectorSearch { collection, vector, k } => {
                Ok(PhysicalPlan::VectorSearch {
                    collection: collection.clone(),
                    vector: vector.clone(),
                    k: *k,
                })
            }
        }
    }

    fn optimize_select(
        &self,
        columns: &[String],
        from: &str,
        filter: &Option<crate::parser::Expr>,
        order_by: &Option<Vec<(String, bool)>>,
        limit: &Option<usize>,
    ) -> Result<PhysicalPlan> {
        // Build execution plan
        let mut plan = PhysicalPlan::Scan {
            table: from.to_string(),
            columns: columns.to_vec(),
            estimated_cost: 1000.0,
        };

        // Add filter if present
        if let Some(expr) = filter {
            plan = PhysicalPlan::Filter {
                input: Box::new(plan),
                predicate: expr.clone(),
                estimated_cost: 100.0,
            };
        }

        // Add sort if present
        if let Some(order) = order_by {
            plan = PhysicalPlan::Sort {
                input: Box::new(plan),
                order_by: order.clone(),
                estimated_cost: 500.0,
            };
        }

        // Add limit if present
        if let Some(n) = limit {
            plan = PhysicalPlan::Limit {
                input: Box::new(plan),
                limit: *n,
                estimated_cost: 1.0,
            };
        }

        Ok(plan)
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Physical execution plan
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Table scan
    Scan {
        table: String,
        columns: Vec<String>,
        estimated_cost: f64,
    },
    /// Filter (WHERE clause)
    Filter {
        input: Box<PhysicalPlan>,
        predicate: crate::parser::Expr,
        estimated_cost: f64,
    },
    /// Sort (ORDER BY)
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<(String, bool)>,
        estimated_cost: f64,
    },
    /// Limit
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        estimated_cost: f64,
    },
    /// Insert
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<serde_json::Value>>,
        estimated_cost: f64,
    },
    /// Update
    Update {
        table: String,
        set: std::collections::HashMap<String, serde_json::Value>,
        filter: Option<crate::parser::Expr>,
        estimated_cost: f64,
    },
    /// Delete
    Delete {
        table: String,
        filter: Option<crate::parser::Expr>,
        estimated_cost: f64,
    },
    /// Create table
    CreateTable {
        name: String,
        columns: Vec<crate::parser::ColumnDef>,
    },
    /// Drop table
    DropTable {
        name: String,
    },
    /// Stream from topic
    Stream {
        topic: String,
        filter: Option<crate::parser::Expr>,
        limit: Option<usize>,
    },
    /// Create topic
    TopicCreate {
        name: String,
        partitions: u32,
        replication: u32,
    },
    /// List topics
    TopicList,
    /// Delete topic
    TopicDelete {
        name: String,
    },
    /// Vector search
    VectorSearch {
        collection: String,
        vector: Vec<f32>,
        k: usize,
    },
}

impl PhysicalPlan {
    /// Convert to QueryPlan for EXPLAIN
    pub fn to_query_plan(&self) -> QueryPlan {
        let nodes = self.to_plan_nodes();
        let cost = self.estimated_cost();

        QueryPlan {
            nodes,
            estimated_cost: cost,
            estimated_rows: 100, // Placeholder
        }
    }

    fn to_plan_nodes(&self) -> Vec<lumadb_common::types::PlanNode> {
        match self {
            PhysicalPlan::Scan { table, columns, estimated_cost } => {
                vec![lumadb_common::types::PlanNode {
                    node_type: "Scan".to_string(),
                    description: format!("Scan {} columns from {}", columns.len(), table),
                    children: vec![],
                    cost: *estimated_cost,
                    rows: 1000,
                }]
            }
            PhysicalPlan::Filter { input, predicate, estimated_cost } => {
                let mut nodes = input.to_plan_nodes();
                nodes.push(lumadb_common::types::PlanNode {
                    node_type: "Filter".to_string(),
                    description: format!("Filter with predicate"),
                    children: vec![],
                    cost: *estimated_cost,
                    rows: 100,
                });
                nodes
            }
            _ => vec![lumadb_common::types::PlanNode {
                node_type: "Unknown".to_string(),
                description: "Unknown operation".to_string(),
                children: vec![],
                cost: 0.0,
                rows: 0,
            }],
        }
    }

    fn estimated_cost(&self) -> f64 {
        match self {
            PhysicalPlan::Scan { estimated_cost, .. } => *estimated_cost,
            PhysicalPlan::Filter { input, estimated_cost, .. } => {
                input.estimated_cost() + estimated_cost
            }
            PhysicalPlan::Sort { input, estimated_cost, .. } => {
                input.estimated_cost() + estimated_cost
            }
            PhysicalPlan::Limit { input, estimated_cost, .. } => {
                input.estimated_cost() + estimated_cost
            }
            PhysicalPlan::Insert { estimated_cost, .. } => *estimated_cost,
            PhysicalPlan::Update { estimated_cost, .. } => *estimated_cost,
            PhysicalPlan::Delete { estimated_cost, .. } => *estimated_cost,
            _ => 0.0,
        }
    }
}
