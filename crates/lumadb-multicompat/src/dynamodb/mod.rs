//! AWS DynamoDB HTTP API Server
//!
//! Full DynamoDB API compatible server using Axum.

pub mod auth;
pub mod translator;

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde_json::{json, Map, Value as JsonValue};
use tracing::{debug, error, info, instrument};

use crate::core::{
    AdapterError, AttributeDefinition, AttributeType, BatchOperation, KeySchema,
    QueryFilter, Row, StorageEngine, TableSchema, TransactWriteItem, Value,
};

use auth::{validate_signature, AuthConfig};
use translator::{
    build_query_filter, dynamodb_item_to_row, extract_key_from_item,
    parse_dynamodb_value, row_to_dynamodb_item, value_to_dynamodb,
};

/// DynamoDB configuration
#[derive(Debug, Clone)]
pub struct DynamoDBConfig {
    pub region: String,
    pub port: u16,
    pub auth: AuthConfig,
}

impl Default for DynamoDBConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            port: 8000,
            auth: AuthConfig::default(),
        }
    }
}

/// DynamoDB API server
pub struct DynamoDBServer {
    storage: Arc<dyn StorageEngine>,
    config: DynamoDBConfig,
}

impl DynamoDBServer {
    /// Create a new DynamoDB server
    pub fn new(storage: Arc<dyn StorageEngine>, config: DynamoDBConfig) -> Self {
        Self { storage, config }
    }

    /// Create Axum router
    pub fn router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route("/", post(handle_request))
            .with_state(state)
    }
}

/// Main request handler
#[instrument(skip(state, body))]
async fn handle_request(
    State(state): State<Arc<DynamoDBServer>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, DynamoDBError> {
    let start = Instant::now();

    // Validate authentication
    validate_signature(&headers, &body, &state.config.auth)
        .map_err(|e| DynamoDBError::from(e))?;

    // Get operation from x-amz-target header
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing x-amz-target header".into()))?;

    let operation = target
        .split('.')
        .nth(1)
        .ok_or_else(|| DynamoDBError::InvalidRequest("Invalid x-amz-target format".into()))?;

    debug!("DynamoDB operation: {}", operation);

    // Parse request body
    let request: JsonValue = serde_json::from_slice(&body)
        .map_err(|e| DynamoDBError::InvalidRequest(format!("Invalid JSON: {}", e)))?;

    // Route to handler
    let response = match operation {
        "PutItem" => handle_put_item(&state.storage, request).await?,
        "GetItem" => handle_get_item(&state.storage, request).await?,
        "DeleteItem" => handle_delete_item(&state.storage, request).await?,
        "UpdateItem" => handle_update_item(&state.storage, request).await?,
        "Query" => handle_query(&state.storage, request).await?,
        "Scan" => handle_scan(&state.storage, request).await?,
        "BatchWriteItem" => handle_batch_write_item(&state.storage, request).await?,
        "BatchGetItem" => handle_batch_get_item(&state.storage, request).await?,
        "CreateTable" => handle_create_table(&state.storage, request).await?,
        "DeleteTable" => handle_delete_table(&state.storage, request).await?,
        "DescribeTable" => handle_describe_table(&state.storage, request).await?,
        "ListTables" => handle_list_tables(&state.storage, request).await?,
        "TransactWriteItems" => handle_transact_write(&state.storage, request).await?,
        _ => return Err(DynamoDBError::UnknownOperation(operation.into())),
    };

    info!("DynamoDB {} completed in {:?}", operation, start.elapsed());
    Ok((StatusCode::OK, Json(response)).into_response())
}

// ===== Handler Implementations =====

async fn handle_put_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let item = get_required_object(&request, "Item")?;

    let key = extract_key_from_item(item)?;
    let row = dynamodb_item_to_row(item)?;

    // Check condition expression if present
    if let Some(_condition) = request.get("ConditionExpression") {
        // TODO: Evaluate condition against existing item
        debug!("ConditionExpression present, skipping validation");
    }

    storage.execute_kv_put(&table_name, key, row).await?;

    Ok(json!({}))
}

async fn handle_get_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let key = get_required_object(&request, "Key")?;

    let key_value = extract_key_from_item(key)?;
    let result = storage.execute_kv_get(&table_name, key_value).await?;

    match result {
        Some(row) => Ok(json!({
            "Item": row_to_dynamodb_item(&row)
        })),
        None => Ok(json!({})),
    }
}

async fn handle_delete_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let key = get_required_object(&request, "Key")?;

    let key_value = extract_key_from_item(key)?;

    // Get old item if ReturnValues specified
    let return_values = request.get("ReturnValues").and_then(|v| v.as_str());
    let old_item = if return_values == Some("ALL_OLD") {
        storage.execute_kv_get(&table_name, key_value.clone()).await?
    } else {
        None
    };

    storage.execute_kv_delete(&table_name, key_value).await?;

    match old_item {
        Some(row) => Ok(json!({
            "Attributes": row_to_dynamodb_item(&row)
        })),
        None => Ok(json!({})),
    }
}

async fn handle_update_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let key = get_required_object(&request, "Key")?;
    let key_value = extract_key_from_item(key)?;

    // Get existing item
    let existing = storage.execute_kv_get(&table_name, key_value.clone()).await?;
    let mut row = existing.unwrap_or_else(Row::new);

    // Apply UpdateExpression
    if let Some(expr) = request.get("UpdateExpression").and_then(|v| v.as_str()) {
        let attr_values = request.get("ExpressionAttributeValues").and_then(|v| v.as_object());
        let attr_names = request.get("ExpressionAttributeNames").and_then(|v| v.as_object());
        
        apply_update_expression(&mut row, expr, attr_values, attr_names)?;
    }

    storage.execute_kv_put(&table_name, key_value, row.clone()).await?;

    let return_values = request.get("ReturnValues").and_then(|v| v.as_str());
    if return_values == Some("ALL_NEW") {
        Ok(json!({
            "Attributes": row_to_dynamodb_item(&row)
        }))
    } else {
        Ok(json!({}))
    }
}

async fn handle_query(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let filter = build_query_filter(&request)?;
    
    let rows = storage.execute_kv_query(&table_name, filter).await?;
    let items: Vec<JsonValue> = rows.iter().map(row_to_dynamodb_item).collect();

    Ok(json!({
        "Items": items,
        "Count": items.len(),
        "ScannedCount": items.len()
    }))
}

async fn handle_scan(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    
    let limit = request.get("Limit").and_then(|v| v.as_u64()).map(|l| l as usize);
    let filter = QueryFilter {
        limit,
        ..Default::default()
    };
    
    let rows = storage.execute_kv_query(&table_name, filter).await?;
    let items: Vec<JsonValue> = rows.iter().map(row_to_dynamodb_item).collect();

    Ok(json!({
        "Items": items,
        "Count": items.len(),
        "ScannedCount": items.len()
    }))
}

async fn handle_batch_write_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let request_items = request.get("RequestItems")
        .and_then(|v| v.as_object())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing RequestItems".into()))?;

    let mut operations = Vec::new();

    for (table_name, requests) in request_items {
        let reqs = requests.as_array()
            .ok_or_else(|| DynamoDBError::InvalidRequest("RequestItems values must be arrays".into()))?;

        for req in reqs {
            if let Some(put_req) = req.get("PutRequest") {
                let item = put_req.get("Item")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| DynamoDBError::InvalidRequest("PutRequest missing Item".into()))?;
                
                let key = extract_key_from_item(item)?;
                let row = dynamodb_item_to_row(item)?;
                
                operations.push(BatchOperation::Put {
                    table: table_name.clone(),
                    key,
                    value: row,
                });
            }

            if let Some(delete_req) = req.get("DeleteRequest") {
                let key_obj = delete_req.get("Key")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| DynamoDBError::InvalidRequest("DeleteRequest missing Key".into()))?;
                
                let key = extract_key_from_item(key_obj)?;
                
                operations.push(BatchOperation::Delete {
                    table: table_name.clone(),
                    key,
                });
            }
        }
    }

    storage.batch_write(operations).await?;

    Ok(json!({
        "UnprocessedItems": {}
    }))
}

async fn handle_batch_get_item(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let request_items = request.get("RequestItems")
        .and_then(|v| v.as_object())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing RequestItems".into()))?;

    let mut responses: Map<String, JsonValue> = Map::new();

    for (table_name, table_req) in request_items {
        let keys = table_req.get("Keys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| DynamoDBError::InvalidRequest("Missing Keys".into()))?;

        let mut items = Vec::new();
        for key_obj in keys {
            let key_map = key_obj.as_object()
                .ok_or_else(|| DynamoDBError::InvalidRequest("Key must be object".into()))?;
            
            let key = extract_key_from_item(key_map)?;
            if let Some(row) = storage.execute_kv_get(table_name, key).await? {
                items.push(row_to_dynamodb_item(&row));
            }
        }

        responses.insert(table_name.clone(), json!(items));
    }

    Ok(json!({
        "Responses": responses,
        "UnprocessedKeys": {}
    }))
}

async fn handle_transact_write(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let items = request.get("TransactItems")
        .and_then(|v| v.as_array())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing TransactItems".into()))?;

    let mut operations = Vec::new();

    for item in items {
        if let Some(put) = item.get("Put") {
            let table = put.get("TableName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| DynamoDBError::InvalidRequest("Put missing TableName".into()))?;
            
            let item_obj = put.get("Item")
                .and_then(|v| v.as_object())
                .ok_or_else(|| DynamoDBError::InvalidRequest("Put missing Item".into()))?;

            let key = extract_key_from_item(item_obj)?;
            let row = dynamodb_item_to_row(item_obj)?;

            operations.push(TransactWriteItem {
                operation: BatchOperation::Put {
                    table: table.to_string(),
                    key,
                    value: row,
                },
                condition_expression: put.get("ConditionExpression").and_then(|v| v.as_str()).map(String::from),
                expression_values: None,
            });
        }

        if let Some(delete) = item.get("Delete") {
            let table = delete.get("TableName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| DynamoDBError::InvalidRequest("Delete missing TableName".into()))?;
            
            let key_obj = delete.get("Key")
                .and_then(|v| v.as_object())
                .ok_or_else(|| DynamoDBError::InvalidRequest("Delete missing Key".into()))?;

            let key = extract_key_from_item(key_obj)?;

            operations.push(TransactWriteItem {
                operation: BatchOperation::Delete {
                    table: table.to_string(),
                    key,
                },
                condition_expression: delete.get("ConditionExpression").and_then(|v| v.as_str()).map(String::from),
                expression_values: None,
            });
        }
    }

    storage.transact_write(operations).await?;

    Ok(json!({}))
}

async fn handle_create_table(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    
    let key_schema_arr = request.get("KeySchema")
        .and_then(|v| v.as_array())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing KeySchema".into()))?;

    let partition_key = key_schema_arr.iter()
        .find(|k| k.get("KeyType").and_then(|v| v.as_str()) == Some("HASH"))
        .and_then(|k| k.get("AttributeName"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| DynamoDBError::InvalidRequest("Missing HASH key".into()))?
        .to_string();

    let sort_key = key_schema_arr.iter()
        .find(|k| k.get("KeyType").and_then(|v| v.as_str()) == Some("RANGE"))
        .and_then(|k| k.get("AttributeName"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let schema = TableSchema {
        name: table_name.clone(),
        key_schema: KeySchema { partition_key: partition_key.clone(), sort_key: sort_key.clone() },
        attributes: vec![
            AttributeDefinition { name: partition_key.clone(), attribute_type: AttributeType::S },
        ],
        global_secondary_indexes: vec![],
        local_secondary_indexes: vec![],
    };

    storage.create_table(&table_name, schema).await?;

    Ok(json!({
        "TableDescription": {
            "TableName": table_name,
            "TableStatus": "ACTIVE",
            "KeySchema": key_schema_arr
        }
    }))
}

async fn handle_delete_table(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    storage.delete_table(&table_name).await?;

    Ok(json!({
        "TableDescription": {
            "TableName": table_name,
            "TableStatus": "DELETING"
        }
    }))
}

async fn handle_describe_table(
    storage: &Arc<dyn StorageEngine>,
    request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let table_name = get_required_string(&request, "TableName")?;
    let schema = storage.describe_table(&table_name).await?;

    Ok(json!({
        "Table": {
            "TableName": schema.name,
            "TableStatus": "ACTIVE",
            "KeySchema": [
                {"AttributeName": schema.key_schema.partition_key, "KeyType": "HASH"}
            ]
        }
    }))
}

async fn handle_list_tables(
    storage: &Arc<dyn StorageEngine>,
    _request: JsonValue,
) -> Result<JsonValue, DynamoDBError> {
    let tables = storage.list_tables().await?;
    
    Ok(json!({
        "TableNames": tables
    }))
}

// ===== Helper Functions =====

fn get_required_string(request: &JsonValue, field: &str) -> Result<String, DynamoDBError> {
    request.get(field)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| DynamoDBError::InvalidRequest(format!("Missing required field: {}", field)))
}

fn get_required_object<'a>(request: &'a JsonValue, field: &str) -> Result<&'a Map<String, JsonValue>, DynamoDBError> {
    request.get(field)
        .and_then(|v| v.as_object())
        .ok_or_else(|| DynamoDBError::InvalidRequest(format!("Missing required field: {}", field)))
}

fn apply_update_expression(
    row: &mut Row,
    expr: &str,
    attr_values: Option<&Map<String, JsonValue>>,
    _attr_names: Option<&Map<String, JsonValue>>,
) -> Result<(), DynamoDBError> {
    // Simple SET expression parser
    let expr = expr.trim();
    
    if expr.to_uppercase().starts_with("SET ") {
        let set_expr = &expr[4..];
        for assignment in set_expr.split(',') {
            let parts: Vec<&str> = assignment.split('=').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let attr_name = parts[0].trim();
                let value_ref = parts[1].trim();

                if value_ref.starts_with(':') {
                    if let Some(values) = attr_values {
                        if let Some(ddb_val) = values.get(value_ref) {
                            let value = parse_dynamodb_value(ddb_val)?;
                            
                            // Update or add the attribute
                            if let Some(existing) = row.get_mut(attr_name) {
                                *existing = value;
                            } else {
                                row.push(attr_name, value);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// ===== Error Types =====

#[derive(Debug, thiserror::Error)]
pub enum DynamoDBError {
    #[error("ValidationException: {0}")]
    InvalidRequest(String),

    #[error("UnknownOperationException: {0}")]
    UnknownOperation(String),

    #[error("ResourceNotFoundException: {0}")]
    ResourceNotFound(String),

    #[error("ConditionalCheckFailedException: {0}")]
    ConditionCheckFailed(String),

    #[error("InternalServerError: {0}")]
    InternalError(String),
}

impl From<AdapterError> for DynamoDBError {
    fn from(e: AdapterError) -> Self {
        match e {
            AdapterError::NotFound(msg) => DynamoDBError::ResourceNotFound(msg),
            AdapterError::InvalidRequest(msg) => DynamoDBError::InvalidRequest(msg),
            AdapterError::ConditionCheckFailed(msg) => DynamoDBError::ConditionCheckFailed(msg),
            AdapterError::AuthenticationError(msg) => DynamoDBError::InvalidRequest(msg),
            _ => DynamoDBError::InternalError(e.to_string()),
        }
    }
}

impl IntoResponse for DynamoDBError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self {
            DynamoDBError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "ValidationException"),
            DynamoDBError::UnknownOperation(_) => (StatusCode::BAD_REQUEST, "UnknownOperationException"),
            DynamoDBError::ResourceNotFound(_) => (StatusCode::BAD_REQUEST, "ResourceNotFoundException"),
            DynamoDBError::ConditionCheckFailed(_) => (StatusCode::BAD_REQUEST, "ConditionalCheckFailedException"),
            DynamoDBError::InternalError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError"),
        };

        let body = json!({
            "__type": error_type,
            "message": self.to_string()
        });

        (status, Json(body)).into_response()
    }
}
