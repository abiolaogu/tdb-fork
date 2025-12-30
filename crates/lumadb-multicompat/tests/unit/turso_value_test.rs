//! Turso value type unit tests

use lumadb_multicompat::Value;
use serde_json::json;

// ===== Turso Value Type Structure Tests =====

#[test]
fn test_turso_null_type() {
    let turso_val = json!({"type": "null", "value": null});
    assert_eq!(turso_val["type"], "null");
    assert!(turso_val["value"].is_null());
}

#[test]
fn test_turso_integer_type() {
    let turso_val = json!({"type": "integer", "value": "42"});
    assert_eq!(turso_val["type"], "integer");
    assert_eq!(turso_val["value"], "42");
}

#[test]
fn test_turso_float_type() {
    let turso_val = json!({"type": "float", "value": 3.14159});
    assert_eq!(turso_val["type"], "float");
}

#[test]
fn test_turso_text_type() {
    let turso_val = json!({"type": "text", "value": "hello world"});
    assert_eq!(turso_val["type"], "text");
    assert_eq!(turso_val["value"], "hello world");
}

#[test]
fn test_turso_blob_type() {
    let turso_val = json!({"type": "blob", "base64": "aGVsbG8="});
    assert_eq!(turso_val["type"], "blob");
    assert_eq!(turso_val["base64"], "aGVsbG8=");
}

// ===== Value to Turso Format Conversion =====

fn value_to_turso(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => json!({"type": "null", "value": null}),
        Value::Bool(b) => json!({"type": "integer", "value": if *b { "1" } else { "0" }}),
        Value::Integer(i) => json!({"type": "integer", "value": i.to_string()}),
        Value::Float(f) => json!({"type": "float", "value": *f}),
        Value::String(s) => json!({"type": "text", "value": s}),
        Value::Bytes(b) => json!({"type": "blob", "base64": base64::encode(b)}),
        _ => json!({"type": "null", "value": null}),
    }
}

fn turso_to_value(v: &serde_json::Value) -> Option<Value> {
    let type_name = v["type"].as_str()?;
    match type_name {
        "null" => Some(Value::Null),
        "integer" => {
            let str_val = v["value"].as_str()?;
            Some(Value::Integer(str_val.parse().ok()?))
        }
        "float" => {
            let float_val = v["value"].as_f64()?;
            Some(Value::Float(float_val))
        }
        "text" => {
            let str_val = v["value"].as_str()?;
            Some(Value::String(str_val.to_string()))
        }
        "blob" => {
            let base64_val = v["base64"].as_str()?;
            Some(Value::Bytes(base64::decode(base64_val).ok()?))
        }
        _ => None,
    }
}

#[test]
fn test_value_null_to_turso() {
    let turso = value_to_turso(&Value::Null);
    assert_eq!(turso["type"], "null");
}

#[test]
fn test_value_integer_to_turso() {
    let turso = value_to_turso(&Value::Integer(123));
    assert_eq!(turso["type"], "integer");
    assert_eq!(turso["value"], "123");
}

#[test]
fn test_value_float_to_turso() {
    let turso = value_to_turso(&Value::Float(3.14));
    assert_eq!(turso["type"], "float");
}

#[test]
fn test_value_string_to_turso() {
    let turso = value_to_turso(&Value::String("test".into()));
    assert_eq!(turso["type"], "text");
    assert_eq!(turso["value"], "test");
}

#[test]
fn test_value_bytes_to_turso() {
    let turso = value_to_turso(&Value::Bytes(b"hello".to_vec()));
    assert_eq!(turso["type"], "blob");
    assert_eq!(turso["base64"], "aGVsbG8=");
}

// ===== Turso Response Format Tests =====

#[test]
fn test_execute_response_format() {
    let response = json!({
        "results": [{
            "cols": [
                {"name": "id", "decltype": "INTEGER"},
                {"name": "name", "decltype": "TEXT"}
            ],
            "rows": [
                [{"type": "integer", "value": "1"}, {"type": "text", "value": "Alice"}],
                [{"type": "integer", "value": "2"}, {"type": "text", "value": "Bob"}]
            ],
            "affected_row_count": 0,
            "last_insert_rowid": null,
            "replication_index": null
        }]
    });

    let results = response["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    
    let result = &results[0];
    let cols = result["cols"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_batch_response_format() {
    let response = json!({
        "results": [
            {"cols": [], "rows": [], "affected_row_count": 1},
            {"cols": [], "rows": [], "affected_row_count": 1},
            {"cols": [{"name": "count", "decltype": "INTEGER"}], "rows": [[{"type": "integer", "value": "2"}]]}
        ]
    });

    let results = response["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_pipeline_response_format() {
    let response = json!({
        "results": [
            {"type": "ok", "response": {"type": "execute", "result": {"affected_row_count": 0}}},
            {"type": "ok", "response": {"type": "execute", "result": {"affected_row_count": 1}}},
            {"type": "ok", "response": {"type": "close"}}
        ]
    });

    let results = response["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["type"], "ok");
    assert_eq!(results[2]["response"]["type"], "close");
}

#[test]
fn test_error_response_format() {
    let response = json!({
        "error": {
            "message": "SQLITE_ERROR: syntax error",
            "code": "SQLITE_ERROR"
        }
    });

    assert!(response["error"].is_object());
    assert!(response["error"]["message"].is_string());
    assert!(response["error"]["code"].is_string());
}

// ===== Named Arguments Tests =====

#[test]
fn test_named_args_format() {
    let request = json!({
        "stmt": {
            "sql": "SELECT * FROM users WHERE id = :id AND name = :name",
            "named_args": [
                {"name": "id", "value": {"type": "integer", "value": "1"}},
                {"name": "name", "value": {"type": "text", "value": "Alice"}}
            ]
        }
    });

    let named_args = request["stmt"]["named_args"].as_array().unwrap();
    assert_eq!(named_args.len(), 2);
    assert_eq!(named_args[0]["name"], "id");
    assert_eq!(named_args[1]["name"], "name");
}

#[test]
fn test_positional_args_format() {
    let request = json!({
        "stmt": {
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "args": [
                {"type": "integer", "value": "1"},
                {"type": "text", "value": "Alice"}
            ]
        }
    });

    let args = request["stmt"]["args"].as_array().unwrap();
    assert_eq!(args.len(), 2);
}

// ===== Column Type Tests =====

#[test]
fn test_column_decltype_integer() {
    let col = json!({"name": "id", "decltype": "INTEGER"});
    assert_eq!(col["decltype"], "INTEGER");
}

#[test]
fn test_column_decltype_text() {
    let col = json!({"name": "name", "decltype": "TEXT"});
    assert_eq!(col["decltype"], "TEXT");
}

#[test]
fn test_column_decltype_real() {
    let col = json!({"name": "price", "decltype": "REAL"});
    assert_eq!(col["decltype"], "REAL");
}

#[test]
fn test_column_decltype_blob() {
    let col = json!({"name": "data", "decltype": "BLOB"});
    assert_eq!(col["decltype"], "BLOB");
}

#[test]
fn test_column_decltype_null() {
    let col = json!({"name": "unknown", "decltype": null});
    assert!(col["decltype"].is_null());
}

// Use base64 crate mock for tests
mod base64 {
    pub fn encode(data: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = String::new();
        for chunk in data.chunks(3) {
            let n = chunk.iter().fold(0u32, |acc, &b| (acc << 8) | b as u32) << (8 * (3 - chunk.len()));
            for i in 0..(chunk.len() + 1) {
                result.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
            }
        }
        while result.len() % 4 != 0 {
            result.push('=');
        }
        result
    }

    pub fn decode(input: &str) -> Result<Vec<u8>, ()> {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = Vec::new();
        let input = input.trim_end_matches('=');
        let mut buf = 0u32;
        let mut bits = 0;
        for c in input.chars() {
            let idx = ALPHABET.iter().position(|&b| b as char == c).ok_or(())?;
            buf = (buf << 6) | idx as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                result.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
        Ok(result)
    }
}
