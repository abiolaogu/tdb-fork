/**
 * LumaDB Type Definitions
 */

// ============================================================================
// Core Types
// ============================================================================

export type DocumentId = string;
export type CollectionName = string;
export type IndexName = string;
export type FieldPath = string;

export interface DocumentData {
  [key: string]: any;
}

export interface StoredDocument {
  _id: DocumentId;
  _rev: number;
  _createdAt: Date;
  _updatedAt: Date;
  _deleted?: boolean;
  data: DocumentData;
}

export interface QueryResult<T = DocumentData> {
  documents: T[];
  count: number;
  totalCount: number;
  executionTime: number;
  queryPlan?: QueryPlan;
}

export interface QueryPlan {
  steps: QueryPlanStep[];
  estimatedCost: number;
  indexesUsed: string[];
}

export interface QueryPlanStep {
  operation: string;
  description: string;
  estimatedRows: number;
  indexUsed?: string;
}

// ============================================================================
// Query Types
// ============================================================================

export type QueryLanguage = 'lql' | 'nql' | 'jql';

export interface ParsedQuery {
  type: QueryType;
  collection: string;
  fields?: string[];
  conditions?: QueryCondition[];
  orderBy?: OrderByClause[];
  limit?: number;
  offset?: number;
  groupBy?: string[];
  having?: QueryCondition[];
  joins?: JoinClause[];
  aggregations?: AggregationClause[];
  data?: DocumentData | DocumentData[];
}

export type QueryType =
  | 'SELECT'
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'CREATE_COLLECTION'
  | 'DROP_COLLECTION'
  | 'CREATE_INDEX'
  | 'DROP_INDEX'
  | 'EXPLAIN'
  | 'COUNT'
  | 'AGGREGATE';

export interface QueryCondition {
  field: string;
  operator: ComparisonOperator;
  value: any;
  logic?: 'AND' | 'OR';
}

export type ComparisonOperator =
  | '='
  | '!='
  | '>'
  | '>='
  | '<'
  | '<='
  | 'LIKE'
  | 'NOT LIKE'
  | 'IN'
  | 'NOT IN'
  | 'BETWEEN'
  | 'IS NULL'
  | 'IS NOT NULL'
  | 'CONTAINS'
  | 'STARTS WITH'
  | 'ENDS WITH'
  | 'MATCHES';

export interface OrderByClause {
  field: string;
  direction: 'ASC' | 'DESC';
}

export interface JoinClause {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  collection: string;
  on: {
    leftField: string;
    rightField: string;
  };
}

export interface AggregationClause {
  function: AggregationFunction;
  field: string;
  alias?: string;
}

export type AggregationFunction =
  | 'COUNT'
  | 'SUM'
  | 'AVG'
  | 'MIN'
  | 'MAX'
  | 'FIRST'
  | 'LAST'
  | 'ARRAY_AGG'
  | 'STRING_AGG';

// ============================================================================
// Index Types
// ============================================================================

export type IndexType = 'btree' | 'hash' | 'fulltext' | 'geo';

export interface IndexDefinition {
  name: IndexName;
  collection: CollectionName;
  fields: FieldPath[];
  type: IndexType;
  unique?: boolean;
  sparse?: boolean;
  options?: IndexOptions;
}

export interface IndexOptions {
  // B-Tree specific
  order?: number;

  // Full-text specific
  language?: string;
  stopWords?: string[];
  stemming?: boolean;

  // Geo specific
  minLat?: number;
  maxLat?: number;
  minLon?: number;
  maxLon?: number;
}

export interface IndexStats {
  name: string;
  size: number;
  depth: number;
  entries: number;
  hitRate: number;
}

// ============================================================================
// Transaction Types
// ============================================================================

export type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE';

export type TransactionStatus =
  | 'ACTIVE'
  | 'COMMITTED'
  | 'ROLLED_BACK'
  | 'FAILED';

export interface TransactionOptions {
  isolationLevel?: IsolationLevel;
  timeout?: number;
  readOnly?: boolean;
}

export interface TransactionLog {
  transactionId: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  collection: string;
  documentId: DocumentId;
  beforeData?: DocumentData;
  afterData?: DocumentData;
  timestamp: Date;
}

// ============================================================================
// Storage Types
// ============================================================================

export type StorageType = 'memory' | 'file' | 'hybrid';

export interface StorageOptions {
  type: StorageType;
  path?: string;
  maxMemory?: number;
  compression?: boolean;
  encryption?: EncryptionOptions;
  cacheSize?: number;
}

export interface EncryptionOptions {
  algorithm: 'aes-256-gcm' | 'aes-256-cbc';
  key: string;
}

export interface StorageStats {
  totalDocuments: number;
  totalSize: number;
  collections: CollectionStats[];
}

export interface CollectionStats {
  name: string;
  documentCount: number;
  size: number;
  indexes: IndexStats[];
  avgDocumentSize: number;
}

// ============================================================================
// Database Configuration
// ============================================================================

export interface DatabaseConfig {
  name: string;
  storage: StorageOptions;
  defaultQueryLanguage?: QueryLanguage;
  maxConnections?: number;
  queryTimeout?: number;
  cacheEnabled?: boolean;
  cacheMaxSize?: number;
  logging?: LoggingConfig;
}

export interface LoggingConfig {
  level: 'debug' | 'info' | 'warn' | 'error';
  destination?: 'console' | 'file' | 'both';
  filePath?: string;
}

// ============================================================================
// Event Types
// ============================================================================

export type DatabaseEvent =
  | 'document:created'
  | 'document:updated'
  | 'document:deleted'
  | 'collection:created'
  | 'collection:dropped'
  | 'index:created'
  | 'index:dropped'
  | 'transaction:started'
  | 'transaction:committed'
  | 'transaction:rolledback'
  | 'query:executed';

export interface EventPayload {
  event: DatabaseEvent;
  timestamp: Date;
  data: any;
}

export type EventHandler = (payload: EventPayload) => void;

// ============================================================================
// Error Types
// ============================================================================

export class LumaError extends Error {
  constructor(
    message: string,
    public code: string,
    public details?: any
  ) {
    super(message);
    this.name = 'LumaError';
  }
}

export class QuerySyntaxError extends LumaError {
  constructor(message: string, public position?: number, public suggestion?: string) {
    super(message, 'QUERY_SYNTAX_ERROR', { position, suggestion });
    this.name = 'QuerySyntaxError';
  }
}

export class DocumentNotFoundError extends LumaError {
  constructor(collection: string, id: DocumentId) {
    super(`Document not found: ${id} in collection ${collection}`, 'DOCUMENT_NOT_FOUND');
    this.name = 'DocumentNotFoundError';
  }
}

export class CollectionNotFoundError extends LumaError {
  constructor(collection: string) {
    super(`Collection not found: ${collection}`, 'COLLECTION_NOT_FOUND');
    this.name = 'CollectionNotFoundError';
  }
}

export class TransactionError extends LumaError {
  constructor(message: string, transactionId?: string) {
    super(message, 'TRANSACTION_ERROR', { transactionId });
    this.name = 'TransactionError';
  }
}

export class IndexError extends LumaError {
  constructor(message: string, indexName?: string) {
    super(message, 'INDEX_ERROR', { indexName });
    this.name = 'IndexError';
  }
}

export class ValidationError extends LumaError {
  constructor(message: string, field?: string) {
    super(message, 'VALIDATION_ERROR', { field });
    this.name = 'ValidationError';
  }
}
