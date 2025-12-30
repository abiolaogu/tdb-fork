//! DynamoDB translator unit tests

use lumadb_multicompat::dynamodb::translator::*;
use lumadb_multicompat::Value;
use serde_json::json;
use std::collections::HashMap;

// ===== String Type Tests =====

#[test]
fn test_parse_string() {
    let ddb = json!({"S": "hello world"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::String("hello world".into()));
}

#[test]
fn test_string_roundtrip() {
    let original = Value::String("test_string".into());
    let ddb = value_to_dynamodb(&original);
    let back = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(original, back);
}

#[test]
fn test_empty_string() {
    let ddb = json!({"S": ""});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::String("".into()));
}

#[test]
fn test_unicode_string() {
    let ddb = json!({"S": "日本語テスト 🎉"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::String("日本語テスト 🎉".into()));
}

// ===== Number Type Tests =====

#[test]
fn test_parse_integer() {
    let ddb = json!({"N": "42"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::Integer(42));
}

#[test]
fn test_parse_negative_integer() {
    let ddb = json!({"N": "-123"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::Integer(-123));
}

#[test]
fn test_parse_float() {
    let ddb = json!({"N": "3.14159"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Float(f) => assert!((f - 3.14159).abs() < 0.00001),
        _ => panic!("Expected float"),
    }
}

#[test]
fn test_parse_scientific_notation() {
    let ddb = json!({"N": "1.5e10"});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Float(f) => assert!((f - 1.5e10).abs() < 1e6),
        Value::Integer(i) => assert_eq!(i, 15000000000),
        _ => panic!("Expected number"),
    }
}

#[test]
fn test_number_roundtrip() {
    let original = Value::Integer(999999999);
    let ddb = value_to_dynamodb(&original);
    let back = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(original, back);
}

// ===== Boolean Type Tests =====

#[test]
fn test_parse_bool_true() {
    let ddb = json!({"BOOL": true});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::Bool(true));
}

#[test]
fn test_parse_bool_false() {
    let ddb = json!({"BOOL": false});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::Bool(false));
}

#[test]
fn test_bool_roundtrip() {
    let original = Value::Bool(true);
    let ddb = value_to_dynamodb(&original);
    let back = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(original, back);
}

// ===== Null Type Tests =====

#[test]
fn test_parse_null() {
    let ddb = json!({"NULL": true});
    let val = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(val, Value::Null);
}

#[test]
fn test_null_roundtrip() {
    let original = Value::Null;
    let ddb = value_to_dynamodb(&original);
    let back = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(original, back);
}

// ===== List Type Tests =====

#[test]
fn test_parse_list() {
    let ddb = json!({"L": [{"S": "a"}, {"N": "1"}, {"BOOL": true}]});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::String("a".into()));
            assert_eq!(arr[1], Value::Integer(1));
            assert_eq!(arr[2], Value::Bool(true));
        }
        _ => panic!("Expected array"),
    }
}

#[test]
fn test_empty_list() {
    let ddb = json!({"L": []});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Array(arr) => assert!(arr.is_empty()),
        _ => panic!("Expected empty array"),
    }
}

#[test]
fn test_nested_list() {
    let ddb = json!({"L": [{"L": [{"S": "nested"}]}]});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                Value::Array(inner) => {
                    assert_eq!(inner[0], Value::String("nested".into()));
                }
                _ => panic!("Expected nested array"),
            }
        }
        _ => panic!("Expected array"),
    }
}

// ===== Map Type Tests =====

#[test]
fn test_parse_map() {
    let ddb = json!({"M": {"name": {"S": "Alice"}, "age": {"N": "30"}}});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Object(map) => {
            assert_eq!(map.get("name"), Some(&Value::String("Alice".into())));
            assert_eq!(map.get("age"), Some(&Value::Integer(30)));
        }
        _ => panic!("Expected object"),
    }
}

#[test]
fn test_empty_map() {
    let ddb = json!({"M": {}});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Object(map) => assert!(map.is_empty()),
        _ => panic!("Expected empty map"),
    }
}

#[test]
fn test_nested_map() {
    let ddb = json!({"M": {"user": {"M": {"id": {"N": "1"}}}}});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Object(map) => {
            let user = map.get("user").unwrap();
            match user {
                Value::Object(inner) => {
                    assert_eq!(inner.get("id"), Some(&Value::Integer(1)));
                }
                _ => panic!("Expected nested map"),
            }
        }
        _ => panic!("Expected object"),
    }
}

// ===== Set Type Tests =====

#[test]
fn test_parse_string_set() {
    let ddb = json!({"SS": ["a", "b", "c"]});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::StringSet(ss) => {
            assert_eq!(ss.len(), 3);
            assert!(ss.contains(&"a".to_string()));
            assert!(ss.contains(&"b".to_string()));
            assert!(ss.contains(&"c".to_string()));
        }
        _ => panic!("Expected string set"),
    }
}

#[test]
fn test_parse_number_set() {
    let ddb = json!({"NS": ["1", "2", "3.5"]});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::NumberSet(ns) => {
            assert_eq!(ns.len(), 3);
        }
        _ => panic!("Expected number set"),
    }
}

#[test]
fn test_parse_binary_set() {
    let ddb = json!({"BS": ["YQ==", "Yg=="]});  // base64 for "a", "b"
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::BinarySet(bs) => {
            assert_eq!(bs.len(), 2);
        }
        _ => panic!("Expected binary set"),
    }
}

// ===== Binary Type Tests =====

#[test]
fn test_parse_binary() {
    let ddb = json!({"B": "aGVsbG8="});  // base64 for "hello"
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Bytes(b) => assert_eq!(b, b"hello"),
        _ => panic!("Expected bytes"),
    }
}

#[test]
fn test_empty_binary() {
    let ddb = json!({"B": ""});
    let val = parse_dynamodb_value(&ddb).unwrap();
    match val {
        Value::Bytes(b) => assert!(b.is_empty()),
        _ => panic!("Expected empty bytes"),
    }
}

// ===== Complex Roundtrip Tests =====

#[test]
fn test_complex_roundtrip() {
    let original = Value::Object(HashMap::from([
        ("string".into(), Value::String("test".into())),
        ("number".into(), Value::Integer(42)),
        ("float".into(), Value::Float(3.14)),
        ("bool".into(), Value::Bool(true)),
        ("null".into(), Value::Null),
        ("array".into(), Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])),
    ]));

    let ddb = value_to_dynamodb(&original);
    let back = parse_dynamodb_value(&ddb).unwrap();
    assert_eq!(original, back);
}

// ===== Key Condition Parsing Tests =====

#[test]
fn test_parse_simple_equality() {
    let expr = "pk = :pk";
    let values = json!({":pk": {"S": "user123"}});
    
    let cond = parse_key_condition(expr, None, values.as_object()).unwrap();
    assert!(cond.is_some());
    
    let cond = cond.unwrap();
    assert_eq!(cond.partition_key.0, "pk");
    assert_eq!(cond.partition_key.1, Value::String("user123".into()));
}

#[test]
fn test_parse_with_attribute_names() {
    let expr = "#pk = :pk";
    let names = json!({"#pk": "partition_key"});
    let values = json!({":pk": {"S": "test"}});
    
    let cond = parse_key_condition(
        expr,
        names.as_object(),
        values.as_object()
    ).unwrap().unwrap();
    
    assert_eq!(cond.partition_key.0, "partition_key");
}

#[test]
fn test_parse_begins_with() {
    let expr = "pk = :pk AND begins_with(sk, :prefix)";
    let values = json!({
        ":pk": {"S": "user123"},
        ":prefix": {"S": "ORDER#"}
    });
    
    let cond = parse_key_condition(expr, None, values.as_object()).unwrap().unwrap();
    assert!(cond.sort_key.is_some());
}
