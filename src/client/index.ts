/**
 * TDB+ JavaScript/TypeScript Client SDK
 *
 * Easy-to-use client for connecting to TDB+ servers.
 * Supports all query languages with fluent API.
 */

import { QueryLanguage, QueryResult, DocumentData } from '../types';

export interface TDBClientConfig {
  url: string;
  defaultLanguage?: QueryLanguage;
  timeout?: number;
  headers?: Record<string, string>;
}

export interface QueryOptions {
  language?: QueryLanguage;
  timeout?: number;
}

/**
 * TDB+ Client for remote database access
 */
export class TDBClient {
  private config: Required<TDBClientConfig>;

  constructor(config: TDBClientConfig | string) {
    if (typeof config === 'string') {
      this.config = {
        url: config,
        defaultLanguage: 'lql',
        timeout: 30000,
        headers: {},
      };
    } else {
      this.config = {
        url: config.url,
        defaultLanguage: config.defaultLanguage || 'lql',
        timeout: config.timeout || 30000,
        headers: config.headers || {},
      };
    }

    // Remove trailing slash
    this.config.url = this.config.url.replace(/\/$/, '');
  }

  // ============================================================================
  // Query Methods
  // ============================================================================

  /**
   * Execute a query using the default language
   */
  async query<T = DocumentData>(
    queryString: string,
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    return this.request<T>('/query', {
      query: queryString,
      language: options?.language || this.config.defaultLanguage,
    });
  }

  /**
   * Execute a TQL (SQL-like) query
   */
  async tql<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.query<T>(queryString, { language: 'lql' });
  }

  /**
   * Execute an NQL (Natural Language) query
   */
  async nql<T = DocumentData>(queryString: string): Promise<QueryResult<T>> {
    return this.query<T>(queryString, { language: 'nql' });
  }

  /**
   * Execute a JQL (JSON) query
   */
  async jql<T = DocumentData>(queryString: string | object): Promise<QueryResult<T>> {
    const query = typeof queryString === 'object' ? JSON.stringify(queryString) : queryString;
    return this.query<T>(query, { language: 'jql' });
  }

  // ============================================================================
  // Fluent Query Builder
  // ============================================================================

  /**
   * Start building a query for a collection
   */
  collection(name: string): QueryBuilder {
    return new QueryBuilder(this, name);
  }

  /**
   * Alias for collection()
   */
  from(name: string): QueryBuilder {
    return this.collection(name);
  }

  // ============================================================================
  // Management Methods
  // ============================================================================

  /**
   * List all collections
   */
  async listCollections(): Promise<string[]> {
    const response = await this.fetch('/collections');
    return response.collections;
  }

  /**
   * Get database statistics
   */
  async getStats(): Promise<any> {
    const response = await this.fetch('/stats');
    return response.stats;
  }

  /**
   * Health check
   */
  async health(): Promise<{ status: string; version: string; uptime: number }> {
    return this.fetch('/health');
  }

  // ============================================================================
  // Internal Methods
  // ============================================================================

  private async request<T>(endpoint: string, body: any): Promise<QueryResult<T>> {
    const response = await this.fetch(endpoint, {
      method: 'POST',
      body: JSON.stringify(body),
    });

    return {
      documents: response.data || [],
      count: response.count || 0,
      totalCount: response.totalCount || 0,
      executionTime: response.executionTime || 0,
      queryPlan: response.queryPlan,
    };
  }

  private async fetch(endpoint: string, options: RequestInit = {}): Promise<any> {
    const url = `${this.config.url}${endpoint}`;

    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...this.config.headers,
        ...options.headers,
      },
    });

    const data = await response.json();

    if (!response.ok || (data as any).success === false) {
      const error = (data as any).error || { message: 'Unknown error' };
      throw new TDBClientError(error.message, error.code, response.status);
    }

    return data;
  }
}

/**
 * Fluent query builder for easier query construction
 */
export class QueryBuilder {
  private client: TDBClient;
  private collectionName: string;
  private conditions: Array<{ field: string; op: string; value: any }>;
  private orderByFields: Array<{ field: string; direction: 'ASC' | 'DESC' }>;
  private limitValue?: number;
  private offsetValue?: number;
  private selectFields?: string[];

  constructor(client: TDBClient, collectionName: string) {
    this.client = client;
    this.collectionName = collectionName;
    this.conditions = [];
    this.orderByFields = [];
  }

  /**
   * Select specific fields
   */
  select(...fields: string[]): this {
    this.selectFields = fields;
    return this;
  }

  /**
   * Add a where condition
   */
  where(field: string, operator: string, value: any): this;
  where(field: string, value: any): this;
  where(field: string, operatorOrValue: any, value?: any): this {
    if (value === undefined) {
      this.conditions.push({ field, op: '=', value: operatorOrValue });
    } else {
      this.conditions.push({ field, op: operatorOrValue, value });
    }
    return this;
  }

  /**
   * Add equals condition
   */
  eq(field: string, value: any): this {
    return this.where(field, '=', value);
  }

  /**
   * Add not equals condition
   */
  ne(field: string, value: any): this {
    return this.where(field, '!=', value);
  }

  /**
   * Add greater than condition
   */
  gt(field: string, value: any): this {
    return this.where(field, '>', value);
  }

  /**
   * Add greater than or equal condition
   */
  gte(field: string, value: any): this {
    return this.where(field, '>=', value);
  }

  /**
   * Add less than condition
   */
  lt(field: string, value: any): this {
    return this.where(field, '<', value);
  }

  /**
   * Add less than or equal condition
   */
  lte(field: string, value: any): this {
    return this.where(field, '<=', value);
  }

  /**
   * Add LIKE condition
   */
  like(field: string, pattern: string): this {
    return this.where(field, 'LIKE', pattern);
  }

  /**
   * Add IN condition
   */
  in(field: string, values: any[]): this {
    return this.where(field, 'IN', values);
  }

  /**
   * Add BETWEEN condition
   */
  between(field: string, low: any, high: any): this {
    return this.where(field, 'BETWEEN', [low, high]);
  }

  /**
   * Order by a field
   */
  orderBy(field: string, direction: 'ASC' | 'DESC' = 'ASC'): this {
    this.orderByFields.push({ field, direction });
    return this;
  }

  /**
   * Limit results
   */
  limit(count: number): this {
    this.limitValue = count;
    return this;
  }

  /**
   * Skip results (offset)
   */
  offset(count: number): this {
    this.offsetValue = count;
    return this;
  }

  /**
   * Alias for offset
   */
  skip(count: number): this {
    return this.offset(count);
  }

  /**
   * Execute the query and get results
   */
  async get<T = DocumentData>(): Promise<T[]> {
    const result = await this.execute<T>();
    return result.documents;
  }

  /**
   * Execute and get first result
   */
  async first<T = DocumentData>(): Promise<T | null> {
    this.limitValue = 1;
    const results = await this.get<T>();
    return results[0] || null;
  }

  /**
   * Execute and get count
   */
  async count(): Promise<number> {
    const result = await this.execute();
    return result.totalCount;
  }

  /**
   * Execute the query
   */
  async execute<T = DocumentData>(): Promise<QueryResult<T>> {
    const query = this.buildTQL();
    return this.client.tql<T>(query);
  }

  /**
   * Insert documents
   */
  async insert(data: DocumentData | DocumentData[]): Promise<QueryResult> {
    const docs = Array.isArray(data) ? data : [data];
    const jqlQuery = {
      insert: this.collectionName,
      documents: docs,
    };
    return this.client.jql(jqlQuery);
  }

  /**
   * Update documents
   */
  async update(data: DocumentData): Promise<QueryResult> {
    const filter: Record<string, any> = {};
    for (const cond of this.conditions) {
      filter[cond.field] = this.buildJQLCondition(cond.op, cond.value);
    }

    const jqlQuery = {
      update: this.collectionName,
      filter,
      set: data,
    };
    return this.client.jql(jqlQuery);
  }

  /**
   * Delete documents
   */
  async delete(): Promise<QueryResult> {
    const filter: Record<string, any> = {};
    for (const cond of this.conditions) {
      filter[cond.field] = this.buildJQLCondition(cond.op, cond.value);
    }

    const jqlQuery = {
      delete: this.collectionName,
      filter,
    };
    return this.client.jql(jqlQuery);
  }

  // ============================================================================
  // Query Building
  // ============================================================================

  private buildTQL(): string {
    const parts: string[] = [];

    // SELECT
    const fields = this.selectFields?.join(', ') || '*';
    parts.push(`SELECT ${fields} FROM ${this.collectionName}`);

    // WHERE
    if (this.conditions.length > 0) {
      const whereParts = this.conditions.map((c) => {
        const value = this.formatValue(c.value);
        if (c.op === 'IN') {
          return `${c.field} IN (${(c.value as any[]).map(this.formatValue).join(', ')})`;
        }
        if (c.op === 'BETWEEN') {
          return `${c.field} BETWEEN ${this.formatValue(c.value[0])} AND ${this.formatValue(c.value[1])}`;
        }
        return `${c.field} ${c.op} ${value}`;
      });
      parts.push(`WHERE ${whereParts.join(' AND ')}`);
    }

    // ORDER BY
    if (this.orderByFields.length > 0) {
      const orderParts = this.orderByFields.map((o) => `${o.field} ${o.direction}`);
      parts.push(`ORDER BY ${orderParts.join(', ')}`);
    }

    // LIMIT
    if (this.limitValue !== undefined) {
      parts.push(`LIMIT ${this.limitValue}`);
    }

    // OFFSET
    if (this.offsetValue !== undefined) {
      parts.push(`OFFSET ${this.offsetValue}`);
    }

    return parts.join(' ');
  }

  private buildJQLCondition(op: string, value: any): any {
    const opMap: Record<string, string> = {
      '=': '$eq',
      '!=': '$ne',
      '>': '$gt',
      '>=': '$gte',
      '<': '$lt',
      '<=': '$lte',
      'IN': '$in',
      'LIKE': '$like',
    };

    const jqlOp = opMap[op];
    if (jqlOp) {
      return { [jqlOp]: value };
    }
    return value;
  }

  private formatValue(value: any): string {
    if (value === null) return 'NULL';
    if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    return String(value);
  }
}

/**
 * TDB Client Error
 */
export class TDBClientError extends Error {
  constructor(
    message: string,
    public code: string = 'UNKNOWN',
    public status: number = 500
  ) {
    super(message);
    this.name = 'TDBClientError';
  }
}

// Export default client factory
export function createClient(config: TDBClientConfig | string): TDBClient {
  return new TDBClient(config);
}
