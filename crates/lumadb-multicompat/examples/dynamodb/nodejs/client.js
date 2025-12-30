/**
 * LumaDB DynamoDB Compatibility Layer - Node.js Client Example
 * 
 * This example demonstrates connecting to LumaDB using the official AWS SDK.
 * Simply change the endpoint URL to point to your LumaDB instance.
 */

const { DynamoDBClient, DynamoDBDocumentClient } = require('@aws-sdk/client-dynamodb');
const {
    PutCommand, GetCommand, DeleteCommand, UpdateCommand,
    QueryCommand, ScanCommand, BatchWriteCommand, BatchGetCommand,
    TransactWriteCommand
} = require('@aws-sdk/lib-dynamodb');

// ===== Configuration =====

const LUMADB_ENDPOINT = process.env.LUMADB_ENDPOINT || 'http://localhost:8000/dynamodb';
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

// Create DynamoDB client pointing to LumaDB
const client = new DynamoDBClient({
    endpoint: LUMADB_ENDPOINT,
    region: AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'lumadb',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'lumadb-secret'
    },
    // Connection pooling for high throughput
    maxAttempts: 3,
    retryMode: 'adaptive'
});

// Document client for easier data manipulation
const docClient = DynamoDBDocumentClient.from(client, {
    marshallOptions: {
        removeUndefinedValues: true,
        convertClassInstanceToMap: true
    },
    unmarshallOptions: {
        wrapNumbers: false
    }
});

// ===== CRUD Operations =====

/**
 * Put an item into a table
 */
async function putItem(tableName, item) {
    try {
        await docClient.send(new PutCommand({
            TableName: tableName,
            Item: item
        }));
        console.log(`✓ Put item: ${JSON.stringify(item.pk || item.id)}`);
        return true;
    } catch (error) {
        console.error(`✗ PutItem failed: ${error.message}`);
        throw error;
    }
}

/**
 * Get an item by key
 */
async function getItem(tableName, key) {
    try {
        const response = await docClient.send(new GetCommand({
            TableName: tableName,
            Key: key
        }));
        return response.Item;
    } catch (error) {
        if (error.name === 'ResourceNotFoundException') {
            return null;
        }
        console.error(`✗ GetItem failed: ${error.message}`);
        throw error;
    }
}

/**
 * Delete an item by key
 */
async function deleteItem(tableName, key) {
    try {
        await docClient.send(new DeleteCommand({
            TableName: tableName,
            Key: key
        }));
        console.log(`✓ Deleted item: ${JSON.stringify(key)}`);
        return true;
    } catch (error) {
        console.error(`✗ DeleteItem failed: ${error.message}`);
        throw error;
    }
}

/**
 * Update an item with expressions
 */
async function updateItem(tableName, key, updates) {
    const updateParts = [];
    const expressionValues = {};
    const expressionNames = {};

    Object.entries(updates).forEach(([field, value], index) => {
        const nameKey = `#f${index}`;
        const valueKey = `:v${index}`;
        updateParts.push(`${nameKey} = ${valueKey}`);
        expressionNames[nameKey] = field;
        expressionValues[valueKey] = value;
    });

    try {
        const response = await docClient.send(new UpdateCommand({
            TableName: tableName,
            Key: key,
            UpdateExpression: `SET ${updateParts.join(', ')}`,
            ExpressionAttributeNames: expressionNames,
            ExpressionAttributeValues: expressionValues,
            ReturnValues: 'ALL_NEW'
        }));
        console.log(`✓ Updated item: ${JSON.stringify(key)}`);
        return response.Attributes;
    } catch (error) {
        console.error(`✗ UpdateItem failed: ${error.message}`);
        throw error;
    }
}

// ===== Query Operations =====

/**
 * Query items by partition key with optional filter
 */
async function queryByPartitionKey(tableName, partitionKeyName, partitionKeyValue, options = {}) {
    const params = {
        TableName: tableName,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': partitionKeyName },
        ExpressionAttributeValues: { ':pk': partitionKeyValue },
        Limit: options.limit,
        ScanIndexForward: options.ascending !== false
    };

    // Add sort key condition if provided
    if (options.sortKeyCondition) {
        params.KeyConditionExpression += ` AND ${options.sortKeyCondition.expression}`;
        Object.assign(params.ExpressionAttributeNames, options.sortKeyCondition.names || {});
        Object.assign(params.ExpressionAttributeValues, options.sortKeyCondition.values || {});
    }

    try {
        const response = await docClient.send(new QueryCommand(params));
        return {
            items: response.Items,
            count: response.Count,
            lastKey: response.LastEvaluatedKey
        };
    } catch (error) {
        console.error(`✗ Query failed: ${error.message}`);
        throw error;
    }
}

/**
 * Query with pagination - returns async iterator
 */
async function* queryWithPagination(tableName, partitionKeyName, partitionKeyValue, pageSize = 100) {
    let lastKey = undefined;

    do {
        const params = {
            TableName: tableName,
            KeyConditionExpression: '#pk = :pk',
            ExpressionAttributeNames: { '#pk': partitionKeyName },
            ExpressionAttributeValues: { ':pk': partitionKeyValue },
            Limit: pageSize,
            ExclusiveStartKey: lastKey
        };

        const response = await docClient.send(new QueryCommand(params));
        yield response.Items;
        lastKey = response.LastEvaluatedKey;
    } while (lastKey);
}

// ===== Batch Operations =====

/**
 * Batch write up to 25 items
 */
async function batchWrite(tableName, items) {
    const BATCH_SIZE = 25;
    const batches = [];

    for (let i = 0; i < items.length; i += BATCH_SIZE) {
        batches.push(items.slice(i, i + BATCH_SIZE));
    }

    for (const batch of batches) {
        const requestItems = {
            [tableName]: batch.map(item => ({
                PutRequest: { Item: item }
            }))
        };

        try {
            await docClient.send(new BatchWriteCommand({ RequestItems: requestItems }));
            console.log(`✓ Batch wrote ${batch.length} items`);
        } catch (error) {
            console.error(`✗ BatchWrite failed: ${error.message}`);
            throw error;
        }
    }
}

/**
 * Batch get multiple items
 */
async function batchGet(tableName, keys) {
    const BATCH_SIZE = 100;
    const results = [];

    for (let i = 0; i < keys.length; i += BATCH_SIZE) {
        const batch = keys.slice(i, i + BATCH_SIZE);
        const requestItems = {
            [tableName]: { Keys: batch }
        };

        try {
            const response = await docClient.send(new BatchGetCommand({ RequestItems: requestItems }));
            results.push(...(response.Responses[tableName] || []));
        } catch (error) {
            console.error(`✗ BatchGet failed: ${error.message}`);
            throw error;
        }
    }

    return results;
}

// ===== Transactional Operations =====

/**
 * Transactional write for atomic operations
 */
async function transactWrite(operations) {
    const transactItems = operations.map(op => {
        if (op.type === 'put') {
            return { Put: { TableName: op.tableName, Item: op.item } };
        }
        if (op.type === 'delete') {
            return { Delete: { TableName: op.tableName, Key: op.key } };
        }
        if (op.type === 'update') {
            return {
                Update: {
                    TableName: op.tableName,
                    Key: op.key,
                    UpdateExpression: op.updateExpression,
                    ExpressionAttributeValues: op.expressionValues
                }
            };
        }
    });

    try {
        await docClient.send(new TransactWriteCommand({ TransactItems: transactItems }));
        console.log(`✓ Transaction completed with ${operations.length} operations`);
        return true;
    } catch (error) {
        if (error.name === 'TransactionCanceledException') {
            console.error('Transaction cancelled:', error.CancellationReasons);
        }
        throw error;
    }
}

// ===== Error Handling =====

/**
 * Retry wrapper with exponential backoff
 */
async function withRetry(fn, maxRetries = 3, baseDelay = 100) {
    let lastError;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await fn();
        } catch (error) {
            lastError = error;

            // Don't retry on validation errors
            if (error.name === 'ValidationException') {
                throw error;
            }

            // Exponential backoff
            const delay = baseDelay * Math.pow(2, attempt);
            console.log(`Retry ${attempt + 1}/${maxRetries} after ${delay}ms`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    throw lastError;
}

// ===== Example Usage =====

async function main() {
    const TABLE_NAME = 'Users';

    console.log('=== LumaDB DynamoDB Client Example ===\n');

    // 1. Put items
    console.log('1. Creating users...');
    await putItem(TABLE_NAME, {
        pk: 'USER#001',
        sk: 'PROFILE',
        name: 'Alice Smith',
        email: 'alice@example.com',
        createdAt: new Date().toISOString()
    });

    await putItem(TABLE_NAME, {
        pk: 'USER#001',
        sk: 'ORDER#001',
        product: 'Widget',
        quantity: 5,
        total: 49.99
    });

    // 2. Get item
    console.log('\n2. Fetching user profile...');
    const profile = await getItem(TABLE_NAME, { pk: 'USER#001', sk: 'PROFILE' });
    console.log('Profile:', profile);

    // 3. Update item
    console.log('\n3. Updating user...');
    await updateItem(TABLE_NAME, { pk: 'USER#001', sk: 'PROFILE' }, {
        email: 'alice.smith@example.com',
        updatedAt: new Date().toISOString()
    });

    // 4. Query all items for user
    console.log('\n4. Querying user items...');
    const queryResult = await queryByPartitionKey(TABLE_NAME, 'pk', 'USER#001');
    console.log(`Found ${queryResult.count} items`);

    // 5. Batch write
    console.log('\n5. Batch writing orders...');
    const orders = Array.from({ length: 10 }, (_, i) => ({
        pk: 'USER#001',
        sk: `ORDER#${String(i + 2).padStart(3, '0')}`,
        product: `Product ${i + 2}`,
        total: Math.random() * 100
    }));
    await batchWrite(TABLE_NAME, orders);

    // 6. Query with pagination
    console.log('\n6. Paginated query...');
    for await (const page of queryWithPagination(TABLE_NAME, 'pk', 'USER#001', 5)) {
        console.log(`Page with ${page.length} items`);
    }

    // 7. Transaction
    console.log('\n7. Transactional write...');
    await transactWrite([
        { type: 'put', tableName: TABLE_NAME, item: { pk: 'USER#002', sk: 'PROFILE', name: 'Bob' } },
        { type: 'put', tableName: TABLE_NAME, item: { pk: 'USER#002', sk: 'ORDER#001', total: 25.00 } }
    ]);

    console.log('\n✓ All operations completed successfully!');
}

// Run if executed directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = {
    client,
    docClient,
    putItem,
    getItem,
    deleteItem,
    updateItem,
    queryByPartitionKey,
    queryWithPagination,
    batchWrite,
    batchGet,
    transactWrite,
    withRetry
};
