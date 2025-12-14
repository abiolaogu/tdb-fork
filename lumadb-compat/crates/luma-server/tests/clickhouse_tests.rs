//! Integration tests for ClickHouse HTTP protocol

use reqwest::Client;
use serde_json::Value;

#[tokio::test]
#[ignore] // Run with: cargo test --test clickhouse_tests -- --ignored
async fn test_clickhouse_health() {
    let client = Client::new();
    let resp = client.get("http://127.0.0.1:8123/").send().await;
    
    assert!(resp.is_ok());
    let text = resp.unwrap().text().await.unwrap();
    assert!(text.contains("Ok"));
}

#[tokio::test]
#[ignore]
async fn test_clickhouse_ping() {
    let client = Client::new();
    let resp = client.get("http://127.0.0.1:8123/ping").send().await;
    
    assert!(resp.is_ok());
    let text = resp.unwrap().text().await.unwrap();
    assert!(text.contains("Ok"));
}

#[tokio::test]
#[ignore]
async fn test_clickhouse_query_post() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8123/")
        .body("SELECT 1")
        .send()
        .await;
    
    assert!(resp.is_ok());
    let text = resp.unwrap().text().await.unwrap();
    // Should return result
    assert!(!text.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_clickhouse_json_format() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8123/")
        .body("SELECT 1 as num FORMAT JSON")
        .send()
        .await;
    
    assert!(resp.is_ok());
    let json: Value = resp.unwrap().json().await.unwrap();
    assert!(json.get("data").is_some());
}

#[tokio::test]
#[ignore]
async fn test_clickhouse_csv_format() {
    let client = Client::new();
    let resp = client
        .post("http://127.0.0.1:8123/")
        .body("SELECT 1 as a, 2 as b FORMAT CSVWithNames")
        .send()
        .await;
    
    assert!(resp.is_ok());
    let text = resp.unwrap().text().await.unwrap();
    assert!(text.contains("a,b"));
}
