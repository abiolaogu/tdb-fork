//! DynamoDB JSON format translator
//!
//! Handles conversion between DynamoDB's attribute value format and LumaDB's internal format.

use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;

use crate::core::{AdapterError, Column, KeyCondition, QueryFilter, Row, SortKeyCondition, Value};

/// Convert DynamoDB item to internal Row format
pub fn dynamodb_item_to_row(item: &Map<String, JsonValue>) -> Result<Row, AdapterError> {
    let mut row = Row::with_capacity(item.len());
    for (name, attr_value) in item {
        let value = parse_dynamodb_value(attr_value)?;
        row.push(name, value);
    }
    Ok(row)
}

/// Extract key from DynamoDB item
pub fn extract_key_from_item(item: &Map<String, JsonValue>) -> Result<Value, AdapterError> {
    let mut key_map = HashMap::new();
    for (name, attr_value) in item {
        key_map.insert(name.clone(), parse_dynamodb_value(attr_value)?);
    }
    Ok(Value::Object(key_map))
}

/// Parse DynamoDB attribute value format to internal Value
pub fn parse_dynamodb_value(attr: &JsonValue) -> Result<Value, AdapterError> {
    let obj = attr.as_object().ok_or_else(|| {
        AdapterError::InvalidRequest("Attribute value must be an object".into())
    })?;

    // DynamoDB uses single-key objects like {"S": "value"} or {"N": "123"}
    if let Some((type_key, type_val)) = obj.iter().next() {
        match type_key.as_str() {
            "S" => {
                let s = type_val.as_str().ok_or_else(|| {
                    AdapterError::InvalidRequest("S value must be a string".into())
                })?;
                Ok(Value::String(s.to_string()))
            }
            "N" => {
                let n = type_val.as_str().ok_or_else(|| {
                    AdapterError::InvalidRequest("N value must be a string".into())
                })?;
                // Try integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(AdapterError::InvalidRequest(format!("Invalid number: {}", n)))
                }
            }
            "BOOL" => {
                let b = type_val.as_bool().ok_or_else(|| {
                    AdapterError::InvalidRequest("BOOL value must be a boolean".into())
                })?;
                Ok(Value::Bool(b))
            }
            "NULL" => Ok(Value::Null),
            "B" => {
                let b64 = type_val.as_str().ok_or_else(|| {
                    AdapterError::InvalidRequest("B value must be a base64 string".into())
                })?;
                let bytes = base64::decode(b64).map_err(|e| {
                    AdapterError::InvalidRequest(format!("Invalid base64: {}", e))
                })?;
                Ok(Value::Bytes(bytes))
            }
            "L" => {
                let arr = type_val.as_array().ok_or_else(|| {
                    AdapterError::InvalidRequest("L value must be an array".into())
                })?;
                let values: Result<Vec<Value>, _> = arr.iter().map(parse_dynamodb_value).collect();
                Ok(Value::Array(values?))
            }
            "M" => {
                let map = type_val.as_object().ok_or_else(|| {
                    AdapterError::InvalidRequest("M value must be an object".into())
                })?;
                let mut result = HashMap::new();
                for (k, v) in map {
                    result.insert(k.clone(), parse_dynamodb_value(v)?);
                }
                Ok(Value::Object(result))
            }
            "SS" => {
                let arr = type_val.as_array().ok_or_else(|| {
                    AdapterError::InvalidRequest("SS value must be an array".into())
                })?;
                let strings: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(String::from)
                            .ok_or_else(|| AdapterError::InvalidRequest("SS items must be strings".into()))
                    })
                    .collect();
                Ok(Value::StringSet(strings?))
            }
            "NS" => {
                let arr = type_val.as_array().ok_or_else(|| {
                    AdapterError::InvalidRequest("NS value must be an array".into())
                })?;
                let numbers: Result<Vec<f64>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .ok_or_else(|| AdapterError::InvalidRequest("NS items must be number strings".into()))
                    })
                    .collect();
                Ok(Value::NumberSet(numbers?))
            }
            "BS" => {
                let arr = type_val.as_array().ok_or_else(|| {
                    AdapterError::InvalidRequest("BS value must be an array".into())
                })?;
                let binaries: Result<Vec<Vec<u8>>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| AdapterError::InvalidRequest("BS items must be base64 strings".into()))
                            .and_then(|s| {
                                base64::decode(s).map_err(|e| {
                                    AdapterError::InvalidRequest(format!("Invalid base64: {}", e))
                                })
                            })
                    })
                    .collect();
                Ok(Value::BinarySet(binaries?))
            }
            _ => Err(AdapterError::InvalidRequest(format!("Unknown type: {}", type_key))),
        }
    } else {
        Err(AdapterError::InvalidRequest("Empty attribute value".into()))
    }
}

/// Convert internal Value to DynamoDB attribute value format
pub fn value_to_dynamodb(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({"NULL": true}),
        Value::Bool(b) => json!({"BOOL": b}),
        Value::Integer(i) => json!({"N": i.to_string()}),
        Value::Float(f) => json!({"N": f.to_string()}),
        Value::String(s) => json!({"S": s}),
        Value::Bytes(b) => json!({"B": base64::encode(b)}),
        Value::Array(arr) => {
            let items: Vec<JsonValue> = arr.iter().map(value_to_dynamodb).collect();
            json!({"L": items})
        }
        Value::Object(map) => {
            let items: Map<String, JsonValue> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_dynamodb(v)))
                .collect();
            json!({"M": items})
        }
        Value::StringSet(ss) => json!({"SS": ss}),
        Value::NumberSet(ns) => {
            let strs: Vec<String> = ns.iter().map(|n| n.to_string()).collect();
            json!({"NS": strs})
        }
        Value::BinarySet(bs) => {
            let strs: Vec<String> = bs.iter().map(|b| base64::encode(b)).collect();
            json!({"BS": strs})
        }
    }
}

/// Convert internal Row to DynamoDB item format
pub fn row_to_dynamodb_item(row: &Row) -> JsonValue {
    let mut item = Map::new();
    for col in &row.columns {
        item.insert(col.name.clone(), value_to_dynamodb(&col.value));
    }
    JsonValue::Object(item)
}

/// Parse KeyConditionExpression
pub fn parse_key_condition(
    expression: &str,
    attr_names: Option<&Map<String, JsonValue>>,
    attr_values: Option<&Map<String, JsonValue>>,
) -> Result<Option<KeyCondition>, AdapterError> {
    // Simple expression parser for common patterns
    // Supports: "pk = :pk", "pk = :pk AND sk > :sk", "pk = :pk AND begins_with(sk, :prefix)"
    
    let expr = expression.trim();
    if expr.is_empty() {
        return Ok(None);
    }

    // Split by AND
    let parts: Vec<&str> = expr.split(" AND ").collect();
    
    if parts.is_empty() {
        return Ok(None);
    }

    // Parse partition key condition (first part, must be equality)
    let pk_part = parts[0].trim();
    let (pk_name, pk_value) = parse_equality_condition(pk_part, attr_names, attr_values)?;

    // Parse sort key condition if present
    let sk_condition = if parts.len() > 1 {
        let sk_part = parts[1].trim();
        Some(parse_sort_key_condition(sk_part, attr_names, attr_values)?)
    } else {
        None
    };

    Ok(Some(KeyCondition {
        partition_key: (pk_name, pk_value),
        sort_key: sk_condition,
    }))
}

fn parse_equality_condition(
    expr: &str,
    attr_names: Option<&Map<String, JsonValue>>,
    attr_values: Option<&Map<String, JsonValue>>,
) -> Result<(String, Value), AdapterError> {
    let parts: Vec<&str> = expr.split('=').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(AdapterError::InvalidRequest(format!(
            "Invalid key condition: {}", expr
        )));
    }

    let name = resolve_attribute_name(parts[0], attr_names)?;
    let value = resolve_attribute_value(parts[1], attr_values)?;

    Ok((name, value))
}

fn parse_sort_key_condition(
    expr: &str,
    attr_names: Option<&Map<String, JsonValue>>,
    attr_values: Option<&Map<String, JsonValue>>,
) -> Result<(String, SortKeyCondition), AdapterError> {
    let expr = expr.trim();

    // Check for begins_with function
    if expr.starts_with("begins_with(") {
        let inner = expr
            .strip_prefix("begins_with(")
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| AdapterError::InvalidRequest("Invalid begins_with syntax".into()))?;
        
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return Err(AdapterError::InvalidRequest("begins_with requires 2 arguments".into()));
        }

        let name = resolve_attribute_name(parts[0], attr_names)?;
        let value = resolve_attribute_value(parts[1], attr_values)?;
        
        let prefix = match value {
            Value::String(s) => s,
            _ => return Err(AdapterError::InvalidRequest("begins_with requires string value".into())),
        };

        return Ok((name, SortKeyCondition::BeginsWith(prefix)));
    }

    // Check for BETWEEN
    if expr.contains(" BETWEEN ") {
        let parts: Vec<&str> = expr.split(" BETWEEN ").collect();
        if parts.len() != 2 {
            return Err(AdapterError::InvalidRequest("Invalid BETWEEN syntax".into()));
        }
        
        let name = resolve_attribute_name(parts[0].trim(), attr_names)?;
        let range_parts: Vec<&str> = parts[1].split(" AND ").collect();
        if range_parts.len() != 2 {
            return Err(AdapterError::InvalidRequest("BETWEEN requires two values".into()));
        }

        let low = resolve_attribute_value(range_parts[0].trim(), attr_values)?;
        let high = resolve_attribute_value(range_parts[1].trim(), attr_values)?;

        return Ok((name, SortKeyCondition::Between(low, high)));
    }

    // Parse comparison operators
    for (op, condition_fn) in [
        (">=", SortKeyCondition::GreaterThanOrEqual as fn(Value) -> SortKeyCondition),
        ("<=", SortKeyCondition::LessThanOrEqual as fn(Value) -> SortKeyCondition),
        (">", SortKeyCondition::GreaterThan as fn(Value) -> SortKeyCondition),
        ("<", SortKeyCondition::LessThan as fn(Value) -> SortKeyCondition),
        ("=", SortKeyCondition::Equal as fn(Value) -> SortKeyCondition),
    ] {
        if expr.contains(op) {
            let parts: Vec<&str> = expr.split(op).map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let name = resolve_attribute_name(parts[0], attr_names)?;
                let value = resolve_attribute_value(parts[1], attr_values)?;
                return Ok((name, condition_fn(value)));
            }
        }
    }

    Err(AdapterError::InvalidRequest(format!(
        "Cannot parse sort key condition: {}", expr
    )))
}

fn resolve_attribute_name(
    name: &str,
    attr_names: Option<&Map<String, JsonValue>>,
) -> Result<String, AdapterError> {
    let name = name.trim();
    if name.starts_with('#') {
        attr_names
            .and_then(|m| m.get(name))
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| AdapterError::InvalidRequest(format!(
                "Undefined attribute name: {}", name
            )))
    } else {
        Ok(name.to_string())
    }
}

fn resolve_attribute_value(
    name: &str,
    attr_values: Option<&Map<String, JsonValue>>,
) -> Result<Value, AdapterError> {
    let name = name.trim();
    if name.starts_with(':') {
        attr_values
            .and_then(|m| m.get(name))
            .ok_or_else(|| AdapterError::InvalidRequest(format!(
                "Undefined attribute value: {}", name
            )))
            .and_then(parse_dynamodb_value)
    } else {
        // Literal value - try to parse as string
        Ok(Value::String(name.to_string()))
    }
}

/// Build QueryFilter from DynamoDB request
pub fn build_query_filter(request: &JsonValue) -> Result<QueryFilter, AdapterError> {
    let mut filter = QueryFilter::default();

    // Parse KeyConditionExpression
    if let Some(expr) = request.get("KeyConditionExpression").and_then(|v| v.as_str()) {
        let attr_names = request.get("ExpressionAttributeNames").and_then(|v| v.as_object());
        let attr_values = request.get("ExpressionAttributeValues").and_then(|v| v.as_object());
        filter.key_condition = parse_key_condition(expr, attr_names, attr_values)?;
    }

    // Parse FilterExpression
    if let Some(expr) = request.get("FilterExpression").and_then(|v| v.as_str()) {
        filter.filter_expression = Some(expr.to_string());
    }

    // Parse Limit
    if let Some(limit) = request.get("Limit").and_then(|v| v.as_u64()) {
        filter.limit = Some(limit as usize);
    }

    // Parse ScanIndexForward
    if let Some(forward) = request.get("ScanIndexForward").and_then(|v| v.as_bool()) {
        filter.scan_forward = forward;
    }

    // Parse ConsistentRead
    if let Some(consistent) = request.get("ConsistentRead").and_then(|v| v.as_bool()) {
        filter.consistent_read = consistent;
    }

    // Parse ProjectionExpression
    if let Some(proj) = request.get("ProjectionExpression").and_then(|v| v.as_str()) {
        filter.projection = Some(proj.split(',').map(|s| s.trim().to_string()).collect());
    }

    Ok(filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string_value() {
        let json = json!({"S": "hello"});
        let value = parse_dynamodb_value(&json).unwrap();
        assert_eq!(value, Value::String("hello".into()));
    }

    #[test]
    fn test_parse_number_value() {
        let json = json!({"N": "42"});
        let value = parse_dynamodb_value(&json).unwrap();
        assert_eq!(value, Value::Integer(42));

        let json = json!({"N": "3.14"});
        let value = parse_dynamodb_value(&json).unwrap();
        assert_eq!(value, Value::Float(3.14));
    }

    #[test]
    fn test_parse_bool_value() {
        let json = json!({"BOOL": true});
        let value = parse_dynamodb_value(&json).unwrap();
        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn test_parse_list_value() {
        let json = json!({"L": [{"S": "a"}, {"N": "1"}]});
        let value = parse_dynamodb_value(&json).unwrap();
        match value {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::String("a".into()));
                assert_eq!(arr[1], Value::Integer(1));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_roundtrip_conversion() {
        let original = Value::Object(HashMap::from([
            ("name".into(), Value::String("test".into())),
            ("count".into(), Value::Integer(42)),
        ]));

        let dynamo = value_to_dynamodb(&original);
        let parsed = parse_dynamodb_value(&dynamo).unwrap();
        assert_eq!(original, parsed);
    }
}
