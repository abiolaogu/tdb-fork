//! Query API for observability data
//!
//! Provides a Grafana-compatible query response format for
//! querying traces, metrics, and logs stored in LumaDB.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::storage::ObservabilityStore;

/// Query state for the HTTP API
pub struct QueryState {
    pub store: Arc<ObservabilityStore>,
}

/// Query request matching Grafana Data Source API format
#[derive(Debug, Clone, Deserialize)]
pub struct QueryRequest {
    /// Start time (ISO 8601 or epoch)
    pub from: String,
    /// End time (ISO 8601 or epoch)
    pub to: String,
    /// List of queries to execute
    pub queries: Vec<Query>,
}

/// Individual query within a request
#[derive(Debug, Clone, Deserialize)]
pub struct Query {
    /// Reference ID for correlation in response
    pub ref_id: String,
    /// Data source ID
    pub datasource_id: Option<i64>,
    /// Query expression (PromQL, LogQL, or custom)
    pub expr: String,
    /// Output format (e.g., "time_series", "table")
    pub format: Option<String>,
    /// Maximum data points to return
    pub max_data_points: Option<usize>,
    /// Interval hint in milliseconds
    pub interval_ms: Option<u64>,
}

/// Query response containing results for all queries
#[derive(Debug, Clone, Serialize)]
pub struct QueryResponse {
    /// Results keyed by ref_id
    pub results: HashMap<String, QueryResult>,
}

/// Result for a single query
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    /// Data frames containing the query results
    pub frames: Vec<DataFrame>,
    /// Optional error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// DataFrame representing tabular query results  
#[derive(Debug, Clone, Serialize)]
pub struct DataFrame {
    /// Schema describing the fields
    pub schema: FrameSchema,
    /// Data values
    pub data: FrameData,
}

/// Schema for a DataFrame
#[derive(Debug, Clone, Serialize)]
pub struct FrameSchema {
    /// Field definitions
    pub fields: Vec<FrameField>,
}

/// Field definition within a schema
#[derive(Debug, Clone, Serialize)]
pub struct FrameField {
    /// Field name
    pub name: String,
    /// Field type (e.g., "time", "number", "string")
    #[serde(rename = "type")]
    pub type_: String,
    /// Optional labels for metric identification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,
}

/// Data values for a DataFrame
#[derive(Debug, Clone, Serialize)]
pub struct FrameData {
    /// Column-oriented values (each inner Vec is a column)
    pub values: Vec<Vec<serde_json::Value>>,
}

impl QueryState {
    /// Create a new query state with the given store
    pub fn new(store: Arc<ObservabilityStore>) -> Self {
        Self { store }
    }
}

/// Execute a query against the observability store
pub fn execute_query(
    _state: &QueryState,
    request: QueryRequest,
) -> QueryResponse {
    let mut results = HashMap::new();

    for query in request.queries {
        // Parse expression and execute query
        // This is a stub - real implementation would:
        // 1. Parse PromQL/LogQL/custom expression
        // 2. Execute against the appropriate table
        // 3. Format results

        let frame = DataFrame {
            schema: FrameSchema {
                fields: vec![
                    FrameField {
                        name: "time".into(),
                        type_: "time".into(),
                        labels: None,
                    },
                    FrameField {
                        name: "value".into(),
                        type_: "number".into(),
                        labels: None,
                    },
                ],
            },
            data: FrameData {
                values: vec![
                    // Time column (example timestamps)
                    vec![
                        serde_json::json!(1600000000000i64),
                        serde_json::json!(1600000060000i64),
                    ],
                    // Value column (example values)
                    vec![
                        serde_json::json!(10.0),
                        serde_json::json!(20.0),
                    ],
                ],
            },
        };

        results.insert(
            query.ref_id,
            QueryResult {
                frames: vec![frame],
                error: None,
            },
        );
    }

    QueryResponse { results }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_deserialization() {
        let json = r#"{
            "from": "2024-01-01T00:00:00Z",
            "to": "2024-01-02T00:00:00Z",
            "queries": [
                {
                    "ref_id": "A",
                    "expr": "rate(http_requests_total[5m])"
                }
            ]
        }"#;

        let request: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.queries.len(), 1);
        assert_eq!(request.queries[0].ref_id, "A");
    }

    #[test]
    fn test_query_response_serialization() {
        let response = QueryResponse {
            results: {
                let mut m = HashMap::new();
                m.insert(
                    "A".to_string(),
                    QueryResult {
                        frames: vec![DataFrame {
                            schema: FrameSchema {
                                fields: vec![FrameField {
                                    name: "test".into(),
                                    type_: "number".into(),
                                    labels: None,
                                }],
                            },
                            data: FrameData {
                                values: vec![vec![serde_json::json!(42)]],
                            },
                        }],
                        error: None,
                    },
                );
                m
            },
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"results\""));
    }
}
