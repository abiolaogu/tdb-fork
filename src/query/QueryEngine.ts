/**
 * LumaDB Query Engine
 *
 * Unified query execution engine that supports multiple query languages.
 * Provides query optimization, plan generation, and execution.
 */

import { Database } from '../core/Database';
import { LQLParser } from './parsers/LQLParser';
import { NQLParser } from './parsers/NQLParser';
import { JQLParser } from './parsers/JQLParser';
import {
  QueryLanguage,
  ParsedQuery,
  QueryResult,
  QueryPlan,
  QueryPlanStep,
  DocumentData,
  QuerySyntaxError,
  CollectionNotFoundError,
  AggregationClause,
} from '../types';

export class QueryEngine {
  private database: Database;
  private lqlParser: LQLParser;
  private nqlParser: NQLParser;
  private jqlParser: JQLParser;
  private queryCache: Map<string, ParsedQuery>;
  private cacheMaxSize: number;

  constructor(database: Database) {
    this.database = database;
    this.lqlParser = new LQLParser();
    this.nqlParser = new NQLParser();
    this.jqlParser = new JQLParser();
    this.queryCache = new Map();
    this.cacheMaxSize = 1000;
  }

  /**
   * Execute a query with the specified language
   */
  async execute<T = DocumentData>(
    queryString: string,
    language: QueryLanguage
  ): Promise<QueryResult<T>> {
    const startTime = Date.now();

    // Parse the query
    const parsed = this.parse(queryString, language);

    // Execute based on query type
    let result: QueryResult<T>;

    switch (parsed.type) {
      case 'SELECT':
        result = await this.executeSelect<T>(parsed);
        break;
      case 'INSERT':
        result = await this.executeInsert<T>(parsed);
        break;
      case 'UPDATE':
        result = await this.executeUpdate<T>(parsed);
        break;
      case 'DELETE':
        result = await this.executeDelete<T>(parsed);
        break;
      case 'COUNT':
        result = await this.executeCount<T>(parsed);
        break;
      case 'AGGREGATE':
        result = await this.executeAggregate<T>(parsed);
        break;
      case 'CREATE_COLLECTION':
        result = await this.executeCreateCollection<T>(parsed);
        break;
      case 'DROP_COLLECTION':
        result = await this.executeDropCollection<T>(parsed);
        break;
      case 'CREATE_INDEX':
        result = await this.executeCreateIndex<T>(parsed);
        break;
      case 'DROP_INDEX':
        result = await this.executeDropIndex<T>(parsed);
        break;
      case 'EXPLAIN':
        result = await this.explain<T>(queryString.replace(/^EXPLAIN\s+/i, ''), language);
        break;
      default:
        throw new QuerySyntaxError(`Unknown query type: ${parsed.type}`);
    }

    result.executionTime = Date.now() - startTime;
    return result;
  }

  /**
   * Explain a query without executing it
   */
  async explain<T = DocumentData>(
    queryString: string,
    language: QueryLanguage
  ): Promise<QueryResult<T>> {
    const parsed = this.parse(queryString, language);
    const plan = this.generateQueryPlan(parsed);

    return {
      documents: [] as T[],
      count: 0,
      totalCount: 0,
      executionTime: 0,
      queryPlan: plan,
    };
  }

  /**
   * Parse a query string into a ParsedQuery object
   */
  parse(queryString: string, language: QueryLanguage): ParsedQuery {
    // Check cache
    const cacheKey = `${language}:${queryString}`;
    if (this.queryCache.has(cacheKey)) {
      return this.queryCache.get(cacheKey)!;
    }

    let parsed: ParsedQuery;

    switch (language) {
      case 'lql':
        parsed = this.lqlParser.parse(queryString);
        break;
      case 'nql':
        parsed = this.nqlParser.parse(queryString);
        break;
      case 'jql':
        parsed = this.jqlParser.parse(queryString);
        break;
      default:
        throw new QuerySyntaxError(`Unknown query language: ${language}`);
    }

    // Cache the parsed query
    if (this.queryCache.size >= this.cacheMaxSize) {
      // Remove oldest entry
      const firstKey = this.queryCache.keys().next().value;
      this.queryCache.delete(firstKey);
    }
    this.queryCache.set(cacheKey, parsed);

    return parsed;
  }

  // ============================================================================
  // Query Execution Methods
  // ============================================================================

  private async executeSelect<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    const documents = await collection.find({
      conditions: parsed.conditions,
      orderBy: parsed.orderBy,
      limit: parsed.limit,
      offset: parsed.offset,
      fields: parsed.fields,
    });

    // Get total count (without limit/offset)
    const totalCount = await collection.count(parsed.conditions);

    return {
      documents: documents.map((doc) => doc.toObject() as T),
      count: documents.length,
      totalCount,
      executionTime: 0,
    };
  }

  private async executeInsert<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    const dataToInsert = Array.isArray(parsed.data) ? parsed.data : [parsed.data];
    const documents = await collection.insertMany(dataToInsert as DocumentData[]);

    return {
      documents: documents.map((doc) => doc.toObject() as T),
      count: documents.length,
      totalCount: documents.length,
      executionTime: 0,
    };
  }

  private async executeUpdate<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    if (!parsed.conditions || parsed.conditions.length === 0) {
      throw new QuerySyntaxError('UPDATE requires WHERE conditions for safety');
    }

    const result = await collection.updateMany(
      parsed.conditions,
      parsed.data as DocumentData
    );

    return {
      documents: result.documents.map((doc) => doc.toObject() as T),
      count: result.modified,
      totalCount: result.modified,
      executionTime: 0,
    };
  }

  private async executeDelete<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    if (!parsed.conditions || parsed.conditions.length === 0) {
      throw new QuerySyntaxError('DELETE requires WHERE conditions for safety');
    }

    const deleted = await collection.deleteMany(parsed.conditions);

    return {
      documents: [] as T[],
      count: deleted,
      totalCount: deleted,
      executionTime: 0,
    };
  }

  private async executeCount<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);
    const count = await collection.count(parsed.conditions);

    return {
      documents: [{ count } as unknown as T],
      count: 1,
      totalCount: count,
      executionTime: 0,
    };
  }

  private async executeAggregate<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    // Get all matching documents
    const documents = await collection.find({
      conditions: parsed.conditions,
    });

    const docData = documents.map((d) => d.data);

    // Group by fields if specified
    let groups: Map<string, DocumentData[]>;

    if (parsed.groupBy && parsed.groupBy.length > 0) {
      groups = this.groupDocuments(docData, parsed.groupBy);
    } else {
      groups = new Map([['__all__', docData]]);
    }

    // Apply aggregations
    const results: T[] = [];

    for (const [groupKey, groupDocs] of groups) {
      const result: any = {};

      // Add group by fields
      if (parsed.groupBy && parsed.groupBy.length > 0 && groupKey !== '__all__') {
        const keyParts = groupKey.split('::');
        parsed.groupBy.forEach((field, i) => {
          result[field] = keyParts[i];
        });
      }

      // Calculate aggregations
      for (const agg of parsed.aggregations || []) {
        const aggResult = this.calculateAggregation(groupDocs, agg);
        result[agg.alias || `${agg.function}(${agg.field})`] = aggResult;
      }

      results.push(result);
    }

    return {
      documents: results,
      count: results.length,
      totalCount: results.length,
      executionTime: 0,
    };
  }

  private async executeCreateCollection<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    this.database.collection(parsed.collection);

    return {
      documents: [{ message: `Collection ${parsed.collection} created` } as unknown as T],
      count: 1,
      totalCount: 1,
      executionTime: 0,
    };
  }

  private async executeDropCollection<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const dropped = await this.database.dropCollection(parsed.collection);

    return {
      documents: [
        { message: dropped ? `Collection ${parsed.collection} dropped` : 'Collection not found' } as unknown as T,
      ],
      count: 1,
      totalCount: 1,
      executionTime: 0,
    };
  }

  private async executeCreateIndex<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);

    const indexData = parsed.data as {
      name: string;
      fields: string[];
      type?: string;
      unique?: boolean;
    };

    await collection.createIndex(
      indexData.name,
      indexData.fields,
      (indexData.type as any) || 'btree',
      { unique: indexData.unique }
    );

    return {
      documents: [{ message: `Index ${indexData.name} created` } as unknown as T],
      count: 1,
      totalCount: 1,
      executionTime: 0,
    };
  }

  private async executeDropIndex<T>(parsed: ParsedQuery): Promise<QueryResult<T>> {
    const collection = this.database.collection(parsed.collection);
    const indexName = (parsed.data as { name: string }).name;

    const dropped = await collection.dropIndex(indexName);

    return {
      documents: [
        { message: dropped ? `Index ${indexName} dropped` : 'Index not found' } as unknown as T,
      ],
      count: 1,
      totalCount: 1,
      executionTime: 0,
    };
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private generateQueryPlan(parsed: ParsedQuery): QueryPlan {
    const steps: QueryPlanStep[] = [];
    let estimatedCost = 0;
    const indexesUsed: string[] = [];

    // Check for index usage
    const indexManager = this.database.getIndexManager();
    const usableIndexes: string[] = [];

    if (parsed.conditions) {
      for (const condition of parsed.conditions) {
        const index = indexManager.findIndexForField(parsed.collection, condition.field);
        if (index) {
          usableIndexes.push(index.name);
          indexesUsed.push(index.name);
        }
      }
    }

    // Build query plan steps
    if (usableIndexes.length > 0) {
      steps.push({
        operation: 'INDEX_SCAN',
        description: `Scan index(es): ${usableIndexes.join(', ')}`,
        estimatedRows: 100,
        indexUsed: usableIndexes[0],
      });
      estimatedCost += 10;
    } else {
      steps.push({
        operation: 'FULL_SCAN',
        description: `Full collection scan on ${parsed.collection}`,
        estimatedRows: 1000,
      });
      estimatedCost += 1000;
    }

    if (parsed.conditions && parsed.conditions.length > 0) {
      steps.push({
        operation: 'FILTER',
        description: `Apply ${parsed.conditions.length} condition(s)`,
        estimatedRows: 100,
      });
      estimatedCost += 50;
    }

    if (parsed.orderBy && parsed.orderBy.length > 0) {
      steps.push({
        operation: 'SORT',
        description: `Sort by ${parsed.orderBy.map((o) => `${o.field} ${o.direction}`).join(', ')}`,
        estimatedRows: 100,
      });
      estimatedCost += 200;
    }

    if (parsed.limit) {
      steps.push({
        operation: 'LIMIT',
        description: `Limit to ${parsed.limit} rows`,
        estimatedRows: Math.min(parsed.limit, 100),
      });
    }

    return {
      steps,
      estimatedCost,
      indexesUsed,
    };
  }

  private groupDocuments(
    documents: DocumentData[],
    groupBy: string[]
  ): Map<string, DocumentData[]> {
    const groups = new Map<string, DocumentData[]>();

    for (const doc of documents) {
      const keyParts = groupBy.map((field) => String(this.getNestedValue(doc, field) ?? 'null'));
      const key = keyParts.join('::');

      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(doc);
    }

    return groups;
  }

  private calculateAggregation(documents: DocumentData[], agg: AggregationClause): any {
    const values = documents
      .map((doc) => this.getNestedValue(doc, agg.field))
      .filter((v) => v !== undefined && v !== null);

    switch (agg.function) {
      case 'COUNT':
        return values.length;

      case 'SUM':
        return values.reduce((sum, v) => sum + (Number(v) || 0), 0);

      case 'AVG':
        if (values.length === 0) return null;
        return values.reduce((sum, v) => sum + (Number(v) || 0), 0) / values.length;

      case 'MIN':
        if (values.length === 0) return null;
        return Math.min(...values.map((v) => Number(v)));

      case 'MAX':
        if (values.length === 0) return null;
        return Math.max(...values.map((v) => Number(v)));

      case 'FIRST':
        return values[0] ?? null;

      case 'LAST':
        return values[values.length - 1] ?? null;

      case 'ARRAY_AGG':
        return values;

      case 'STRING_AGG':
        return values.join(', ');

      default:
        return null;
    }
  }

  private getNestedValue(obj: any, path: string): any {
    const parts = path.split('.');
    let current = obj;

    for (const part of parts) {
      if (current === null || current === undefined) {
        return undefined;
      }
      current = current[part];
    }

    return current;
  }
}
