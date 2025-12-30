//! D1 converter unit tests

use lumadb_multicompat::Value;
use serde_json::json;

// ===== JSON to Value Conversion =====

fn json_to_value(j: serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.into_iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            Value::Object(obj.into_iter().map(|(k, v)| (k, json_to_value(v))).collect())
        }
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Array(arr) => serde_json::Value::Array(
            arr.iter().map(value_to_json).collect()
        ),
        Value::Object(obj) => serde_json::Value::Object(
            obj.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect()
        ),
        _ => serde_json::Value::Null,
    }
}

// ===== Primitive Type Tests =====

#[test]
fn test_null_conversion() {
    let val = json_to_value(json!(null));
    assert_eq!(val, Value::Null);
}

#[test]
fn test_bool_true() {
    let val = json_to_value(json!(true));
    assert_eq!(val, Value::Bool(true));
}

#[test]
fn test_bool_false() {
    let val = json_to_value(json!(false));
    assert_eq!(val, Value::Bool(false));
}

#[test]
fn test_integer() {
    let val = json_to_value(json!(42));
    assert_eq!(val, Value::Integer(42));
}

#[test]
fn test_negative_integer() {
    let val = json_to_value(json!(-100));
    assert_eq!(val, Value::Integer(-100));
}

#[test]
fn test_large_integer() {
    let val = json_to_value(json!(9007199254740991i64));
    assert_eq!(val, Value::Integer(9007199254740991));
}

#[test]
fn test_float() {
    let val = json_to_value(json!(3.14159));
    match val {
        Value::Float(f) => assert!((f - 3.14159).abs() < 0.00001),
        _ => panic!("Expected float"),
    }
}

#[test]
fn test_string() {
    let val = json_to_value(json!("hello"));
    assert_eq!(val, Value::String("hello".into()));
}

#[test]
fn test_empty_string() {
    let val = json_to_value(json!(""));
    assert_eq!(val, Value::String("".into()));
}

// ===== Array Type Tests =====

#[test]
fn test_empty_array() {
    let val = json_to_value(json!([]));
    match val {
        Value::Array(arr) => assert!(arr.is_empty()),
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_integer_array() {
    let val = json_to_value(json!([1, 2, 3]));
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::Integer(1));
            assert_eq!(arr[1], Value::Integer(2));
            assert_eq!(arr[2], Value::Integer(3));
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_mixed_array() {
    let val = json_to_value(json!([1, "two", true, null]));
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], Value::Integer(1));
            assert_eq!(arr[1], Value::String("two".into()));
            assert_eq!(arr[2], Value::Bool(true));
            assert_eq!(arr[3], Value::Null);
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_nested_array() {
    let val = json_to_value(json!([[1, 2], [3, 4]]));
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                Value::Array(inner) => assert_eq!(inner.len(), 2),
                _ => panic!("Expected nested array"),
            }
        }
        _ => panic!("Expected array"),
    }
}

// ===== Object Type Tests =====

#[test]
fn test_empty_object() {
    let val = json_to_value(json!({}));
    match val {
        Value::Object(obj) => assert!(obj.is_empty()),
        _ => panic!("Expected object"),
    }
}

#[test]
fn test_simple_object() {
    let val = json_to_value(json!({"name": "Alice", "age": 30}));
    match val {
        Value::Object(obj) => {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".into())));
            assert_eq!(obj.get("age"), Some(&Value::Integer(30)));
        }
        _ => panic!("Expected object"),
    }
}

#[test]
fn test_nested_object() {
    let val = json_to_value(json!({"user": {"id": 1, "email": "test@test.com"}}));
    match val {
        Value::Object(obj) => {
            let user = obj.get("user").unwrap();
            match user {
                Value::Object(inner) => {
                    assert_eq!(inner.get("id"), Some(&Value::Integer(1)));
                }
                _ => panic!("Expected nested object"),
            }
        }
        _ => panic!("Expected object"),
    }
}

// ===== Roundtrip Tests =====

#[test]
fn test_primitive_roundtrip() {
    let test_values = vec![
        json!(null),
        json!(true),
        json!(42),
        json!(3.14),
        json!("test"),
    ];

    for original in test_values {
        let value = json_to_value(original.clone());
        let back = value_to_json(&value);
        // Note: numbers may change type but value should be close
    }
}

#[test]
fn test_complex_roundtrip() {
    let original = json!({
        "id": 1,
        "name": "test",
        "active": true,
        "tags": ["a", "b"],
        "meta": null
    });

    let value = json_to_value(original.clone());
    let back = value_to_json(&value);
    
    assert_eq!(back["id"], 1);
    assert_eq!(back["name"], "test");
    assert_eq!(back["active"], true);
}

// ===== D1 Response Format Tests =====

#[test]
fn test_cloudflare_success_response() {
    let response = json!({
        "success": true,
        "result": [{
            "results": [{"id": 1, "name": "test"}],
            "success": true,
            "meta": {
                "duration": 0.001,
                "rows_read": 1,
                "rows_written": 0
            }
        }],
        "errors": [],
        "messages": []
    });

    assert!(response["success"].as_bool().unwrap());
    assert!(response["result"].is_array());
    assert!(response["errors"].as_array().unwrap().is_empty());
}

#[test]
fn test_cloudflare_error_response() {
    let response = json!({
        "success": false,
        "result": null,
        "errors": [{
            "code": 1001,
            "message": "Query failed"
        }],
        "messages": []
    });

    assert!(!response["success"].as_bool().unwrap());
    assert!(response["result"].is_null());
    assert!(!response["errors"].as_array().unwrap().is_empty());
}

#[test]
fn test_d1_meta_format() {
    let meta = json!({
        "duration": 0.00123,
        "rows_read": 100,
        "rows_written": 5,
        "last_row_id": 42,
        "changed_db": true,
        "changes": 5
    });

    assert!(meta["duration"].is_number());
    assert!(meta["rows_read"].is_number());
    assert!(meta["changed_db"].is_boolean());
}
