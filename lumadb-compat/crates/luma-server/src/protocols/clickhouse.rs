//! ClickHouse HTTP Protocol Implementation
//! Provides ClickHouse-compatible query interface

use warp::Filter;
use std::sync::Arc;
use tracing::{info, debug, error};
use crate::protocols::QueryProcessor;

/// ClickHouse query result format
#[derive(Debug, Clone)]
pub enum OutputFormat {
    TabSeparated,
    TabSeparatedWithNames,
    JSON,
    JSONEachRow,
    CSV,
    CSVWithNames,
    RowBinary,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "JSON" => OutputFormat::JSON,
            "JSONEACHROW" => OutputFormat::JSONEachRow,
            "CSV" => OutputFormat::CSV,
            "CSVWITHNAMES" => OutputFormat::CSVWithNames,
            "ROWBINARY" => OutputFormat::RowBinary,
            "TABSEPARATEDWITHNAMES" => OutputFormat::TabSeparatedWithNames,
            _ => OutputFormat::TabSeparated,
        }
    }
}

/// Format query result based on output format
fn format_result(rows: Vec<Vec<String>>, columns: Vec<String>, format: &OutputFormat) -> String {
    match format {
        OutputFormat::JSON => {
            let mut result = serde_json::json!({
                "meta": columns.iter().map(|c| serde_json::json!({"name": c, "type": "String"})).collect::<Vec<_>>(),
                "data": rows.iter().map(|row| {
                    let mut obj = serde_json::Map::new();
                    for (i, val) in row.iter().enumerate() {
                        if i < columns.len() {
                            obj.insert(columns[i].clone(), serde_json::Value::String(val.clone()));
                        }
                    }
                    serde_json::Value::Object(obj)
                }).collect::<Vec<_>>(),
                "rows": rows.len(),
            });
            serde_json::to_string_pretty(&result).unwrap_or_default()
        }
        OutputFormat::JSONEachRow => {
            rows.iter().map(|row| {
                let mut obj = serde_json::Map::new();
                for (i, val) in row.iter().enumerate() {
                    if i < columns.len() {
                        obj.insert(columns[i].clone(), serde_json::Value::String(val.clone()));
                    }
                }
                serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default()
            }).collect::<Vec<_>>().join("\n")
        }
        OutputFormat::CSV | OutputFormat::CSVWithNames => {
            let mut result = String::new();
            if matches!(format, OutputFormat::CSVWithNames) {
                result.push_str(&columns.join(","));
                result.push('\n');
            }
            for row in rows {
                result.push_str(&row.iter()
                    .map(|v| format!("\"{}\"", v.replace("\"", "\"\"")))
                    .collect::<Vec<_>>()
                    .join(","));
                result.push('\n');
            }
            result
        }
        OutputFormat::TabSeparated | OutputFormat::TabSeparatedWithNames => {
            let mut result = String::new();
            if matches!(format, OutputFormat::TabSeparatedWithNames) {
                result.push_str(&columns.join("\t"));
                result.push('\n');
            }
            for row in rows {
                result.push_str(&row.join("\t"));
                result.push('\n');
            }
            result
        }
        OutputFormat::RowBinary => {
            // Simplified: just return tab separated for now
            rows.iter().map(|r| r.join("\t")).collect::<Vec<_>>().join("\n")
        }
    }
}

/// Parse ClickHouse query and extract format
fn parse_query(query: &str) -> (String, OutputFormat) {
    let query_upper = query.to_uppercase();
    
    // Check for FORMAT clause
    if let Some(idx) = query_upper.rfind(" FORMAT ") {
        let format_str = query[idx + 8..].trim();
        let base_query = query[..idx].to_string();
        return (base_query, OutputFormat::from_str(format_str));
    }
    
    (query.to_string(), OutputFormat::TabSeparated)
}

/// Run ClickHouse HTTP protocol server
pub async fn run(
    port: u16,
    processor: Arc<dyn QueryProcessor + Send + Sync>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let processor = warp::any().map(move || processor.clone());
    
    // GET / - Health check
    let health = warp::path::end()
        .and(warp::get())
        .map(|| "Ok.\n");
    
    // GET /ping - Ping endpoint
    let ping = warp::path("ping")
        .and(warp::get())
        .map(|| "Ok.\n");
    
    // POST / - Query execution
    let query_post = warp::path::end()
        .and(warp::post())
        .and(warp::body::bytes())
        .and(processor.clone())
        .and_then(|body: bytes::Bytes, proc: Arc<dyn QueryProcessor + Send + Sync>| async move {
            let query = String::from_utf8_lossy(&body).to_string();
            debug!("ClickHouse query: {}", query);
            
            let (parsed_query, format) = parse_query(&query);
            
            match proc.process(crate::protocols::QueryRequest {
                query: parsed_query,
                params: vec![],
            }).await {
                Ok(result) => {
                    let columns: Vec<String> = (0..result.rows.first().map(|r| r.len()).unwrap_or(0))
                        .map(|i| format!("col_{}", i))
                        .collect();
                    
                    let rows: Vec<Vec<String>> = result.rows.iter()
                        .map(|row| row.iter().map(|v| format!("{:?}", v)).collect())
                        .collect();
                    
                    let output = format_result(rows, columns, &format);
                    Ok::<_, warp::Rejection>(warp::reply::with_status(output, warp::http::StatusCode::OK))
                }
                Err(e) => {
                    error!("ClickHouse query error: {}", e);
                    Ok(warp::reply::with_status(
                        format!("Code: 62. DB::Exception: {}", e),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            }
        });
    
    // GET /?query= - Query via URL parameter
    let query_get = warp::path::end()
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(processor.clone())
        .and_then(|params: std::collections::HashMap<String, String>, proc: Arc<dyn QueryProcessor + Send + Sync>| async move {
            let query = params.get("query").cloned().unwrap_or_default();
            debug!("ClickHouse query (GET): {}", query);
            
            let (parsed_query, format) = parse_query(&query);
            
            match proc.process(crate::protocols::QueryRequest {
                query: parsed_query,
                params: vec![],
            }).await {
                Ok(result) => {
                    let columns: Vec<String> = (0..result.rows.first().map(|r| r.len()).unwrap_or(0))
                        .map(|i| format!("col_{}", i))
                        .collect();
                    
                    let rows: Vec<Vec<String>> = result.rows.iter()
                        .map(|row| row.iter().map(|v| format!("{:?}", v)).collect())
                        .collect();
                    
                    let output = format_result(rows, columns, &format);
                    Ok::<_, warp::Rejection>(warp::reply::with_status(output, warp::http::StatusCode::OK))
                }
                Err(e) => {
                    Ok(warp::reply::with_status(
                        format!("Code: 62. DB::Exception: {}", e),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            }
        });
    
    // GET /replicas_status - Replica status
    let replicas_status = warp::path("replicas_status")
        .and(warp::get())
        .map(|| "Ok.\n");
    
    // GET /play - ClickHouse Play UI placeholder
    let play = warp::path("play")
        .and(warp::get())
        .map(|| {
            warp::reply::html(r#"<!DOCTYPE html>
<html><head><title>LumaDB - ClickHouse Play</title></head>
<body style="font-family: sans-serif; padding: 20px;">
<h1>LumaDB ClickHouse Play</h1>
<textarea id="query" rows="5" cols="60" placeholder="Enter your SQL query..."></textarea>
<br><button onclick="run()">Run Query</button>
<pre id="result"></pre>
<script>
async function run() {
  const q = document.getElementById('query').value;
  const r = await fetch('/?query=' + encodeURIComponent(q + ' FORMAT JSON'));
  document.getElementById('result').textContent = await r.text();
}
</script>
</body></html>"#)
        });
    
    let routes = health
        .or(ping)
        .or(replicas_status)
        .or(play)
        .or(query_post)
        .or(query_get);
    
    info!("ClickHouse HTTP Protocol Server listening on 0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    
    Ok(())
}
