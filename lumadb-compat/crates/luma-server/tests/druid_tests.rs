//! Integration tests for Druid SQL API

use reqwest::Client;
use serde_json::{json, Value};

#[tokio::test]
#[ignore] // Run with: cargo test --test druid_tests -- --ignored
async fn test_druid_status() {
    let client = Client::new();
    let resp = client.get("http://127.0.0.1:8082/status").send().await;
    
    assert!(resp.is_ok());
    let json: Value = resp.unwrap().json().await.unwrap();
    assert!(json.get("version").is_some());
}

#[tokio::test]
#[ignore]
async fn test_druid_sql_query() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8082/druid/v2/sql")
        .json(&json!({
            "query": "SELECT 1 as num",
            "resultFormat": "object"
        }))
        .send()
        .await;
    
    assert!(resp.is_ok());
}

#[tokio::test]
#[ignore]
async fn test_druid_sql_array_format() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8082/druid/v2/sql")
        .json(&json!({
            "query": "SELECT 1, 2, 3",
            "resultFormat": "array",
            "header": true
        }))
        .send()
        .await;
    
    assert!(resp.is_ok());
    let json: Value = resp.unwrap().json().await.unwrap();
    assert!(json.get("columns").is_some() || json.get("rows").is_some());
}

#[tokio::test]
#[ignore]
async fn test_druid_sql_with_params() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8082/druid/v2/sql")
        .json(&json!({
            "query": "SELECT COUNT(*) FROM metrics",
            "parameters": [],
            "context": {
                "timeout": 30000
            }
        }))
        .send()
        .await;
    
    assert!(resp.is_ok());
}
