//! Druid SQL API Protocol Implementation
//! Provides Apache Druid-compatible query interface

use warp::Filter;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tracing::{info, debug, error};
use crate::protocols::QueryProcessor;
use uuid::Uuid;
use chrono::Utc;

/// Druid SQL query request
#[derive(Debug, Deserialize)]
pub struct DruidSqlRequest {
    pub query: String,
    #[serde(default)]
    pub parameters: Vec<DruidParameter>,
    #[serde(default = "default_result_format")]
    #[serde(rename = "resultFormat")]
    pub result_format: String,
    #[serde(default)]
    pub header: bool,
    #[serde(default)]
    pub context: std::collections::HashMap<String, serde_json::Value>,
}

fn default_result_format() -> String {
    "object".to_string()
}

/// Druid query parameter
#[derive(Debug, Deserialize)]
pub struct DruidParameter {
    #[serde(rename = "type")]
    pub param_type: String,
    pub value: serde_json::Value,
}

/// Druid SQL query response
#[derive(Debug, Serialize)]
pub struct DruidSqlResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<DruidColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<serde_json::Map<String, serde_json::Value>>>,
}

#[derive(Debug, Serialize)]
pub struct DruidColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

/// Druid native query request
#[derive(Debug, Deserialize)]
pub struct DruidNativeQuery {
    #[serde(rename = "queryType")]
    pub query_type: String,
    #[serde(rename = "dataSource")]
    pub data_source: String,
    #[serde(default)]
    pub granularity: String,
    #[serde(default)]
    pub intervals: Vec<String>,
    #[serde(default)]
    pub dimensions: Vec<String>,
    #[serde(default)]
    pub aggregations: Vec<DruidAggregation>,
    #[serde(default)]
    pub filter: Option<DruidFilter>,
}

#[derive(Debug, Deserialize)]
pub struct DruidAggregation {
    #[serde(rename = "type")]
    pub agg_type: String,
    pub name: String,
    #[serde(rename = "fieldName")]
    pub field_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DruidFilter {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub dimension: Option<String>,
    pub value: Option<String>,
}

/// Convert Druid native query to SQL
fn native_to_sql(query: &DruidNativeQuery) -> String {
    let mut sql = String::from("SELECT ");
    
    // Add aggregations
    let aggs: Vec<String> = query.aggregations.iter().map(|agg| {
        match agg.agg_type.as_str() {
            "count" => format!("COUNT(*) AS {}", agg.name),
            "longSum" | "doubleSum" => format!("SUM({}) AS {}", agg.field_name.as_deref().unwrap_or("*"), agg.name),
            "longMax" | "doubleMax" => format!("MAX({}) AS {}", agg.field_name.as_deref().unwrap_or("*"), agg.name),
            "longMin" | "doubleMin" => format!("MIN({}) AS {}", agg.field_name.as_deref().unwrap_or("*"), agg.name),
            _ => format!("{} AS {}", agg.field_name.as_deref().unwrap_or("*"), agg.name),
        }
    }).collect();
    
    if aggs.is_empty() {
        sql.push_str("*");
    } else {
        sql.push_str(&aggs.join(", "));
    }
    
    sql.push_str(&format!(" FROM {}", query.data_source));
    
    // Add WHERE clause
    if let Some(filter) = &query.filter {
        if filter.filter_type == "selector" {
            if let (Some(dim), Some(val)) = (&filter.dimension, &filter.value) {
                sql.push_str(&format!(" WHERE {} = '{}'", dim, val));
            }
        }
    }
    
    // Add GROUP BY
    if !query.dimensions.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", query.dimensions.join(", ")));
    }
    
    sql
}

/// Run Druid protocol server
pub async fn run(
    port: u16,
    processor: Arc<dyn QueryProcessor + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let processor = warp::any().map(move || processor.clone());
    
    // GET /status - Health check
    let status = warp::path("status")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({
                "version": "0.23.0-lumadb"
            }))
        });
    
    // POST /druid/v2/sql - SQL API
    let sql_api = warp::path!("druid" / "v2" / "sql")
        .and(warp::post())
        .and(warp::body::json())
        .and(processor.clone())
        .and_then(|req: DruidSqlRequest, proc: Arc<dyn QueryProcessor + Send + Sync>| async move {
            debug!("Druid SQL query: {}", req.query);
            
            match proc.process(crate::protocols::QueryRequest {
                query: req.query.clone(),
                params: vec![],
            }).await {
                Ok(result) => {
                    let columns: Vec<DruidColumn> = (0..result.rows.first().map(|r| r.len()).unwrap_or(0))
                        .map(|i| DruidColumn {
                            name: format!("col_{}", i),
                            column_type: "STRING".to_string(),
                        })
                        .collect();
                    
                    if req.result_format == "array" || req.result_format == "arrayLines" {
                        // Array format
                        let rows: Vec<Vec<serde_json::Value>> = result.rows.iter()
                            .map(|row| row.iter().map(|v| serde_json::json!(format!("{:?}", v))).collect())
                            .collect();
                        
                        Ok::<_, warp::Rejection>(warp::reply::json(&DruidSqlResponse {
                            columns: if req.header { Some(columns) } else { None },
                            rows: Some(rows),
                            data: None,
                        }))
                    } else {
                        // Object format (default)
                        let data: Vec<serde_json::Map<String, serde_json::Value>> = result.rows.iter()
                            .map(|row| {
                                let mut obj = serde_json::Map::new();
                                for (i, val) in row.iter().enumerate() {
                                    obj.insert(format!("col_{}", i), serde_json::json!(format!("{:?}", val)));
                                }
                                obj
                            })
                            .collect();
                        
                        Ok(warp::reply::json(&DruidSqlResponse {
                            columns: if req.header { Some(columns) } else { None },
                            rows: None,
                            data: Some(data),
                        }))
                    }
                }
                Err(e) => {
                    error!("Druid query error: {}", e);
                    Ok(warp::reply::json(&serde_json::json!({
                        "error": e.to_string(),
                        "errorMessage": e.to_string(),
                    })))
                }
            }
        });
    
    // POST /druid/v2 - Native query API
    let native_api = warp::path!("druid" / "v2")
        .and(warp::post())
        .and(warp::body::json())
        .and(processor.clone())
        .and_then(|req: DruidNativeQuery, proc: Arc<dyn QueryProcessor + Send + Sync>| async move {
            let sql = native_to_sql(&req);
            debug!("Druid native query converted to SQL: {}", sql);
            
            match proc.process(crate::protocols::QueryRequest {
                query: sql,
                params: vec![],
            }).await {
                Ok(result) => {
                    let data: Vec<serde_json::Map<String, serde_json::Value>> = result.rows.iter()
                        .map(|row| {
                            let mut obj = serde_json::Map::new();
                            obj.insert("timestamp".to_string(), serde_json::json!("2024-12-14T00:00:00.000Z"));
                            obj.insert("result".to_string(), serde_json::json!({
                                "data": row.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>()
                            }));
                            obj
                        })
                        .collect();
                    
                    Ok::<_, warp::Rejection>(warp::reply::json(&data))
                }
                Err(e) => {
                    Ok(warp::reply::json(&serde_json::json!({
                        "error": e.to_string()
                    })))
                }
            }
        });
    
    // GET /druid/v2/datasources - List data sources
    let datasources = warp::path!("druid" / "v2" / "datasources")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!(["metrics", "traces", "logs"]))
        });
    
    // GET /druid/v2/datasources/:name - Datasource info
    let datasource_info = warp::path!("druid" / "v2" / "datasources" / String)
        .and(warp::get())
        .map(|name: String| {
            warp::reply::json(&serde_json::json!({
                "name": name,
                "properties": {},
                "segments": {
                    "count": 100,
                    "size": 1073741824
                }
            }))
        });
    
    // GET /druid/v2/servers - List servers
    let servers = warp::path!("druid" / "v2" / "servers")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!([{
                "host": "localhost:8082",
                "type": "historical",
                "tier": "_default_tier",
                "maxSize": 107374182400_i64
            }]))
        });
    
    // GET /health - Health check
    let health = warp::path("health")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({"status": true}))
        });
    
    // POST /druid/v2/sql/statements - Async SQL (Druid 0.24+)
    let sql_statements = warp::path!("druid" / "v2" / "sql" / "statements")
        .and(warp::post())
        .and(warp::body::json())
        .map(|req: DruidSqlRequest| {
            warp::reply::json(&serde_json::json!({
                "queryId": format!("query-{}", uuid::Uuid::new_v4()),
                "state": "RUNNING",
                "createdAt": chrono::Utc::now().to_rfc3339(),
                "query": req.query,
            }))
        });
    
    // POST /druid/indexer/v1/task - Submit ingestion task
    let submit_task = warp::path!("druid" / "indexer" / "v1" / "task")
        .and(warp::post())
        .and(warp::body::json::<serde_json::Value>())
        .map(|_body: serde_json::Value| {
            warp::reply::json(&serde_json::json!({
                "task": format!("index_parallel_{}", uuid::Uuid::new_v4())
            }))
        });
    
    // GET /druid/indexer/v1/task/:id/status - Task status
    let task_status = warp::path!("druid" / "indexer" / "v1" / "task" / String / "status")
        .and(warp::get())
        .map(|id: String| {
            warp::reply::json(&serde_json::json!({
                "id": id,
                "status": {
                    "status": "SUCCESS",
                    "duration": 1000
                }
            }))
        });
    
    let routes = status
        .or(health)
        .or(sql_api)
        .or(native_api)
        .or(datasources)
        .or(datasource_info)
        .or(servers)
        .or(sql_statements)
        .or(submit_task)
        .or(task_status);
    
    info!("Druid Protocol Server listening on 0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    
    Ok(())
}
