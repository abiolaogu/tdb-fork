/**
 * LumaDB - Main Database Class
 *
 * The primary entry point for interacting with LumaDB.
 * Supports multiple query languages and provides enterprise-grade features.
 */

import { v4 as uuidv4 } from 'uuid';
import { Collection } from './Collection';
import { Transaction } from './Transaction';
import { QueryEngine } from '../query/QueryEngine';
import { IndexManager } from '../indexing/IndexManager';
import { StorageEngine } from '../storage/StorageEngine';
import { MemoryStorage } from '../storage/MemoryStorage';
import { FileStorage } from '../storage/FileStorage';
import {
  DatabaseConfig,
  QueryLanguage,
  QueryResult,
  TransactionOptions,
  StorageStats,
  DatabaseEvent,
  EventHandler,
  EventPayload,
  CollectionNotFoundError,
  LumaError,
  DocumentData,
} from '../types';

export class Database {
  private name: string;
  private storage: StorageEngine;
  private collections: Map<string, Collection>;
  private queryEngine: QueryEngine;
  private indexManager: IndexManager;
  private eventHandlers: Map<DatabaseEvent, Set<EventHandler>>;
  private activeTransactions: Map<string, Transaction>;
  private config: DatabaseConfig;
  private isOpen: boolean;
  private defaultQueryLanguage: QueryLanguage;

  constructor(config: DatabaseConfig) {
    this.config = config;
    this.name = config.name;
    this.collections = new Map();
    this.eventHandlers = new Map();
    this.activeTransactions = new Map();
    this.isOpen = false;
    this.defaultQueryLanguage = config.defaultQueryLanguage || 'lql';

    // Initialize storage based on config
    this.storage = this.initializeStorage(config);

    // Initialize query engine
    this.queryEngine = new QueryEngine(this);

    // Initialize index manager
    this.indexManager = new IndexManager();
  }

  private initializeStorage(config: DatabaseConfig): StorageEngine {
    switch (config.storage.type) {
      case 'memory':
        return new MemoryStorage(config.storage);
      case 'file':
        return new FileStorage(config.storage);
      case 'hybrid':
        // Hybrid uses memory with file persistence
        return new FileStorage({ ...config.storage, cacheSize: config.storage.cacheSize || 1000 });
      default:
        return new MemoryStorage(config.storage);
    }
  }

  // ============================================================================
  // Database Lifecycle
  // ============================================================================

  /**
   * Open the database connection
   */
  async open(): Promise<void> {
    if (this.isOpen) {
      return;
    }

    await this.storage.initialize();

    // Load existing collections from storage
    const collectionNames = await this.storage.listCollections();
    for (const name of collectionNames) {
      const collection = new Collection(name, this.storage, this.indexManager, this);
      await collection.load();
      this.collections.set(name, collection);
    }

    this.isOpen = true;
    console.log(`LumaDB Database "${this.name}" opened successfully`);
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    if (!this.isOpen) {
      return;
    }

    // Rollback any active transactions
    for (const [id, transaction] of this.activeTransactions) {
      try {
        await transaction.rollback();
      } catch (e) {
        console.warn(`Failed to rollback transaction ${id}:`, e);
      }
    }
    this.activeTransactions.clear();

    // Flush all collections
    for (const collection of this.collections.values()) {
      await collection.flush();
    }

    await this.storage.close();
    this.isOpen = false;
    console.log(`LumaDB Database "${this.name}" closed`);
  }

  // ============================================================================
  // Collection Management
  // ============================================================================

  /**
   * Get or create a collection
   */
  collection(name: string): Collection {
    this.ensureOpen();

    if (!this.collections.has(name)) {
      const collection = new Collection(name, this.storage, this.indexManager, this);
      this.collections.set(name, collection);
      this.emit('collection:created', { collection: name });
    }

    return this.collections.get(name)!;
  }

  /**
   * Check if a collection exists
   */
  hasCollection(name: string): boolean {
    return this.collections.has(name);
  }

  /**
   * Get all collection names
   */
  getCollectionNames(): string[] {
    return Array.from(this.collections.keys());
  }

  /**
   * Drop a collection
   */
  async dropCollection(name: string): Promise<boolean> {
    this.ensureOpen();

    if (!this.collections.has(name)) {
      return false;
    }

    const collection = this.collections.get(name)!;
    await collection.drop();
    this.collections.delete(name);
    await this.storage.dropCollection(name);

    this.emit('collection:dropped', { collection: name });
    return true;
  }

  // ============================================================================
  // Query Execution - Multiple Languages
  // ============================================================================

  /**
   * Execute a query using the default query language
   */
  async query<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.queryWithLanguage<T>(queryString, this.defaultQueryLanguage);
  }

  /**
   * Execute a LQL (Luma Query Language) query - SQL-like syntax
   *
   * Examples:
   *   SELECT * FROM users WHERE age > 21
   *   INSERT INTO users (name, age) VALUES ('John', 25)
   *   UPDATE users SET status = 'active' WHERE id = '123'
   */
  async lql<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.queryWithLanguage<T>(queryString, 'lql');
  }

  /**
   * Execute an NQL (Natural Query Language) query - Human-readable syntax
   *
   * Examples:
   *   find all users where age is greater than 21
   *   get users with name containing "John" sorted by age descending
   *   count orders where status equals "pending"
   */
  async nql<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.queryWithLanguage<T>(queryString, 'nql');
  }

  /**
   * Execute a JQL (JSON Query Language) query - MongoDB-like syntax
   *
   * Examples:
   *   { "find": "users", "filter": { "age": { "$gt": 21 } } }
   *   { "insert": "users", "documents": [{ "name": "John", "age": 25 }] }
   */
  async jql<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.queryWithLanguage<T>(queryString, 'jql');
  }

  /**
   * Execute a query with a specific language
   */
  async queryWithLanguage<T = DocumentData>(
    queryString: string,
    language: QueryLanguage
  ): Promise<QueryResult<T>> {
    this.ensureOpen();

    const startTime = Date.now();
    const result = await this.queryEngine.execute<T>(queryString, language);

    this.emit('query:executed', {
      query: queryString,
      language,
      executionTime: Date.now() - startTime,
      resultCount: result.count,
    });

    return result;
  }

  /**
   * Explain a query without executing it
   */
  async explain(queryString: string, language?: QueryLanguage): Promise<QueryResult> {
    this.ensureOpen();
    return this.queryEngine.explain(queryString, language || this.defaultQueryLanguage);
  }

  // ============================================================================
  // Transaction Management
  // ============================================================================

  /**
   * Start a new transaction
   */
  async beginTransaction(options?: TransactionOptions): Promise<Transaction> {
    this.ensureOpen();

    const transaction = new Transaction(
      uuidv4(),
      this,
      options || { isolationLevel: 'READ_COMMITTED' }
    );

    this.activeTransactions.set(transaction.id, transaction);
    this.emit('transaction:started', { transactionId: transaction.id });

    return transaction;
  }

  /**
   * Execute a function within a transaction (auto-commit on success, rollback on error)
   */
  async transaction<T>(
    fn: (tx: Transaction) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    const tx = await this.beginTransaction(options);

    try {
      const result = await fn(tx);
      await tx.commit();
      return result;
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  }

  /**
   * Called by Transaction when committed
   * @internal
   */
  _onTransactionCommit(transactionId: string): void {
    this.activeTransactions.delete(transactionId);
    this.emit('transaction:committed', { transactionId });
  }

  /**
   * Called by Transaction when rolled back
   * @internal
   */
  _onTransactionRollback(transactionId: string): void {
    this.activeTransactions.delete(transactionId);
    this.emit('transaction:rolledback', { transactionId });
  }

  // ============================================================================
  // Event System
  // ============================================================================

  /**
   * Subscribe to database events
   */
  on(event: DatabaseEvent, handler: EventHandler): () => void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }

    this.eventHandlers.get(event)!.add(handler);

    // Return unsubscribe function
    return () => {
      this.eventHandlers.get(event)?.delete(handler);
    };
  }

  /**
   * Emit an event
   */
  emit(event: DatabaseEvent, data: any): void {
    const handlers = this.eventHandlers.get(event);
    if (handlers) {
      const payload: EventPayload = {
        event,
        timestamp: new Date(),
        data,
      };

      for (const handler of handlers) {
        try {
          handler(payload);
        } catch (e) {
          console.error(`Error in event handler for ${event}:`, e);
        }
      }
    }
  }

  // ============================================================================
  // Statistics and Management
  // ============================================================================

  /**
   * Get database statistics
   */
  async getStats(): Promise<StorageStats> {
    this.ensureOpen();
    return this.storage.getStats();
  }

  /**
   * Get index manager
   */
  getIndexManager(): IndexManager {
    return this.indexManager;
  }

  /**
   * Get the storage engine
   */
  getStorage(): StorageEngine {
    return this.storage;
  }

  /**
   * Get database name
   */
  getName(): string {
    return this.name;
  }

  /**
   * Set default query language
   */
  setDefaultQueryLanguage(language: QueryLanguage): void {
    this.defaultQueryLanguage = language;
  }

  /**
   * Get default query language
   */
  getDefaultQueryLanguage(): QueryLanguage {
    return this.defaultQueryLanguage;
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  private ensureOpen(): void {
    if (!this.isOpen) {
      throw new LumaError('Database is not open', 'DATABASE_NOT_OPEN');
    }
  }

  /**
   * Create a database with default configuration
   */
  static create(name: string): Database {
    return new Database({
      name,
      storage: { type: 'memory' },
    });
  }

  /**
   * Create a persistent database
   */
  static createPersistent(name: string, path: string): Database {
    return new Database({
      name,
      storage: { type: 'file', path },
    });
  }
}
