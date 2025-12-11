/**
 * TDB+ LQL Parser (TDB Query Language)
 *
 * SQL-like query language for users familiar with traditional databases.
 * Designed to be intuitive and easy to learn.
 *
 * Examples:
 *   SELECT * FROM users WHERE age > 21 ORDER BY name ASC LIMIT 10
 *   INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 25)
 *   UPDATE users SET status = 'active' WHERE id = '123'
 *   DELETE FROM users WHERE inactive = true
 *   SELECT name, COUNT(*) FROM orders GROUP BY name HAVING COUNT(*) > 5
 */

import {
  ParsedQuery,
  QueryType,
  QueryCondition,
  OrderByClause,
  AggregationClause,
  ComparisonOperator,
  QuerySyntaxError,
} from '../../types';

interface Token {
  type: string;
  value: string;
  position: number;
}

export class LQLParser {
  private tokens: Token[];
  private position: number;
  private input: string;

  constructor() {
    this.tokens = [];
    this.position = 0;
    this.input = '';
  }

  /**
   * Parse a LQL query string
   */
  parse(query: string): ParsedQuery {
    this.input = query.trim();
    this.tokens = this.tokenize(this.input);
    this.position = 0;

    if (this.tokens.length === 0) {
      throw new QuerySyntaxError('Empty query');
    }

    const firstToken = this.peek();
    const keyword = firstToken.value.toUpperCase();

    switch (keyword) {
      case 'SELECT':
        return this.parseSelect();
      case 'INSERT':
        return this.parseInsert();
      case 'UPDATE':
        return this.parseUpdate();
      case 'DELETE':
        return this.parseDelete();
      case 'CREATE':
        return this.parseCreate();
      case 'DROP':
        return this.parseDrop();
      case 'EXPLAIN':
        return this.parseExplain();
      default:
        throw new QuerySyntaxError(
          `Unknown command: ${keyword}. Did you mean SELECT, INSERT, UPDATE, or DELETE?`,
          firstToken.position,
          'Try: SELECT * FROM collection_name'
        );
    }
  }

  // ============================================================================
  // Statement Parsers
  // ============================================================================

  private parseSelect(): ParsedQuery {
    this.consume('SELECT');

    // Parse field list or *
    const fields = this.parseFieldList();

    // Check for aggregations
    const aggregations = this.extractAggregations(fields);

    this.consume('FROM');
    const collection = this.consumeIdentifier();

    const result: ParsedQuery = {
      type: aggregations.length > 0 ? 'AGGREGATE' : 'SELECT',
      collection,
      fields: aggregations.length > 0 ? undefined : fields,
      aggregations: aggregations.length > 0 ? aggregations : undefined,
    };

    // Parse optional clauses
    if (this.matchKeyword('WHERE')) {
      result.conditions = this.parseWhereClause();
    }

    if (this.matchKeyword('GROUP')) {
      this.consume('BY');
      result.groupBy = this.parseGroupByClause();
      result.type = 'AGGREGATE';
    }

    if (this.matchKeyword('HAVING')) {
      result.having = this.parseWhereClause();
    }

    if (this.matchKeyword('ORDER')) {
      this.consume('BY');
      result.orderBy = this.parseOrderByClause();
    }

    if (this.matchKeyword('LIMIT')) {
      result.limit = this.consumeNumber();
    }

    if (this.matchKeyword('OFFSET')) {
      result.offset = this.consumeNumber();
    }

    return result;
  }

  private parseInsert(): ParsedQuery {
    this.consume('INSERT');
    this.consume('INTO');
    const collection = this.consumeIdentifier();

    // Parse column list if present
    let columns: string[] | undefined;
    if (this.match('(')) {
      columns = this.parseColumnList();
    }

    this.consume('VALUES');

    // Parse values
    const values = this.parseValuesList();

    // Map columns to values
    const documents = values.map((valueSet) => {
      const doc: Record<string, any> = {};
      if (columns) {
        columns.forEach((col, i) => {
          doc[col] = valueSet[i];
        });
      } else {
        valueSet.forEach((val, i) => {
          doc[`field${i}`] = val;
        });
      }
      return doc;
    });

    return {
      type: 'INSERT',
      collection,
      data: documents.length === 1 ? documents[0] : documents,
    };
  }

  private parseUpdate(): ParsedQuery {
    this.consume('UPDATE');
    const collection = this.consumeIdentifier();
    this.consume('SET');

    // Parse set clauses
    const data: Record<string, any> = {};
    do {
      const field = this.consumeIdentifier();
      this.consume('=');
      const value = this.consumeValue();
      data[field] = value;
    } while (this.match(','));

    const result: ParsedQuery = {
      type: 'UPDATE',
      collection,
      data,
    };

    if (this.matchKeyword('WHERE')) {
      result.conditions = this.parseWhereClause();
    }

    return result;
  }

  private parseDelete(): ParsedQuery {
    this.consume('DELETE');
    this.consume('FROM');
    const collection = this.consumeIdentifier();

    const result: ParsedQuery = {
      type: 'DELETE',
      collection,
    };

    if (this.matchKeyword('WHERE')) {
      result.conditions = this.parseWhereClause();
    }

    return result;
  }

  private parseCreate(): ParsedQuery {
    this.consume('CREATE');

    if (this.matchKeyword('COLLECTION') || this.matchKeyword('TABLE')) {
      const collection = this.consumeIdentifier();
      return {
        type: 'CREATE_COLLECTION',
        collection,
      };
    }

    if (this.matchKeyword('INDEX')) {
      const indexName = this.consumeIdentifier();
      this.consume('ON');
      const collection = this.consumeIdentifier();
      this.consume('(');
      const fields = this.parseColumnList();

      let unique = false;
      if (this.matchKeyword('UNIQUE')) {
        unique = true;
      }

      return {
        type: 'CREATE_INDEX',
        collection,
        data: { name: indexName, fields, unique },
      };
    }

    throw new QuerySyntaxError('Expected COLLECTION, TABLE, or INDEX after CREATE');
  }

  private parseDrop(): ParsedQuery {
    this.consume('DROP');

    if (this.matchKeyword('COLLECTION') || this.matchKeyword('TABLE')) {
      const collection = this.consumeIdentifier();
      return {
        type: 'DROP_COLLECTION',
        collection,
      };
    }

    if (this.matchKeyword('INDEX')) {
      const indexName = this.consumeIdentifier();
      this.consume('ON');
      const collection = this.consumeIdentifier();
      return {
        type: 'DROP_INDEX',
        collection,
        data: { name: indexName },
      };
    }

    throw new QuerySyntaxError('Expected COLLECTION, TABLE, or INDEX after DROP');
  }

  private parseExplain(): ParsedQuery {
    this.consume('EXPLAIN');
    return {
      type: 'EXPLAIN',
      collection: '',
    };
  }

  // ============================================================================
  // Clause Parsers
  // ============================================================================

  private parseFieldList(): string[] {
    if (this.match('*')) {
      return ['*'];
    }

    const fields: string[] = [];
    do {
      const field = this.consumeFieldExpression();
      fields.push(field);
    } while (this.match(','));

    return fields;
  }

  private parseColumnList(): string[] {
    const columns: string[] = [];
    do {
      columns.push(this.consumeIdentifier());
    } while (this.match(','));
    this.consume(')');
    return columns;
  }

  private parseValuesList(): any[][] {
    const allValues: any[][] = [];

    do {
      this.consume('(');
      const values: any[] = [];
      do {
        values.push(this.consumeValue());
      } while (this.match(','));
      this.consume(')');
      allValues.push(values);
    } while (this.match(','));

    return allValues;
  }

  private parseWhereClause(): QueryCondition[] {
    const conditions: QueryCondition[] = [];

    do {
      const condition = this.parseCondition();
      conditions.push(condition);

      if (this.matchKeyword('AND')) {
        // Next condition will have AND logic (default)
      } else if (this.matchKeyword('OR')) {
        // Mark next condition as OR
        if (conditions.length > 0) {
          // Peek at next condition and set its logic
          const nextCondition = this.parseCondition();
          nextCondition.logic = 'OR';
          conditions.push(nextCondition);
        }
      } else {
        break;
      }
    } while (true);

    return conditions;
  }

  private parseCondition(): QueryCondition {
    const field = this.consumeIdentifier();

    // Handle IS NULL / IS NOT NULL
    if (this.matchKeyword('IS')) {
      if (this.matchKeyword('NOT')) {
        this.consume('NULL');
        return { field, operator: 'IS NOT NULL', value: null };
      }
      this.consume('NULL');
      return { field, operator: 'IS NULL', value: null };
    }

    // Handle NOT IN, NOT LIKE
    if (this.matchKeyword('NOT')) {
      if (this.matchKeyword('IN')) {
        const values = this.parseInList();
        return { field, operator: 'NOT IN', value: values };
      }
      if (this.matchKeyword('LIKE')) {
        const pattern = this.consumeValue();
        return { field, operator: 'NOT LIKE', value: pattern };
      }
      throw new QuerySyntaxError('Expected IN or LIKE after NOT');
    }

    // Handle IN
    if (this.matchKeyword('IN')) {
      const values = this.parseInList();
      return { field, operator: 'IN', value: values };
    }

    // Handle LIKE
    if (this.matchKeyword('LIKE')) {
      const pattern = this.consumeValue();
      return { field, operator: 'LIKE', value: pattern };
    }

    // Handle BETWEEN
    if (this.matchKeyword('BETWEEN')) {
      const low = this.consumeValue();
      this.consume('AND');
      const high = this.consumeValue();
      return { field, operator: 'BETWEEN', value: [low, high] };
    }

    // Handle CONTAINS, STARTS WITH, ENDS WITH
    if (this.matchKeyword('CONTAINS')) {
      const value = this.consumeValue();
      return { field, operator: 'CONTAINS', value };
    }

    if (this.matchKeyword('STARTS')) {
      this.consume('WITH');
      const value = this.consumeValue();
      return { field, operator: 'STARTS WITH', value };
    }

    if (this.matchKeyword('ENDS')) {
      this.consume('WITH');
      const value = this.consumeValue();
      return { field, operator: 'ENDS WITH', value };
    }

    if (this.matchKeyword('MATCHES')) {
      const value = this.consumeValue();
      return { field, operator: 'MATCHES', value };
    }

    // Handle comparison operators
    const operator = this.consumeOperator();
    const value = this.consumeValue();

    return { field, operator, value };
  }

  private parseInList(): any[] {
    this.consume('(');
    const values: any[] = [];
    do {
      values.push(this.consumeValue());
    } while (this.match(','));
    this.consume(')');
    return values;
  }

  private parseOrderByClause(): OrderByClause[] {
    const clauses: OrderByClause[] = [];

    do {
      const field = this.consumeIdentifier();
      let direction: 'ASC' | 'DESC' = 'ASC';

      if (this.matchKeyword('DESC') || this.matchKeyword('DESCENDING')) {
        direction = 'DESC';
      } else if (this.matchKeyword('ASC') || this.matchKeyword('ASCENDING')) {
        direction = 'ASC';
      }

      clauses.push({ field, direction });
    } while (this.match(','));

    return clauses;
  }

  private parseGroupByClause(): string[] {
    const fields: string[] = [];
    do {
      fields.push(this.consumeIdentifier());
    } while (this.match(','));
    return fields;
  }

  private extractAggregations(fields: string[]): AggregationClause[] {
    const aggregations: AggregationClause[] = [];
    const aggPattern = /^(COUNT|SUM|AVG|MIN|MAX|FIRST|LAST|ARRAY_AGG|STRING_AGG)\s*\(\s*(\*|[\w.]+)\s*\)(?:\s+AS\s+(\w+))?$/i;

    for (const field of fields) {
      const match = field.match(aggPattern);
      if (match) {
        aggregations.push({
          function: match[1].toUpperCase() as any,
          field: match[2],
          alias: match[3],
        });
      }
    }

    return aggregations;
  }

  // ============================================================================
  // Tokenizer
  // ============================================================================

  private tokenize(input: string): Token[] {
    const tokens: Token[] = [];
    let pos = 0;

    while (pos < input.length) {
      // Skip whitespace
      while (pos < input.length && /\s/.test(input[pos])) {
        pos++;
      }

      if (pos >= input.length) break;

      const start = pos;

      // String literal
      if (input[pos] === "'" || input[pos] === '"') {
        const quote = input[pos];
        pos++;
        let value = '';
        while (pos < input.length && input[pos] !== quote) {
          if (input[pos] === '\\' && pos + 1 < input.length) {
            pos++;
          }
          value += input[pos];
          pos++;
        }
        pos++; // Skip closing quote
        tokens.push({ type: 'STRING', value, position: start });
        continue;
      }

      // Number
      if (/[\d.-]/.test(input[pos])) {
        let value = '';
        while (pos < input.length && /[\d.e+-]/i.test(input[pos])) {
          value += input[pos];
          pos++;
        }
        if (/^-?\d+(\.\d+)?(e[+-]?\d+)?$/i.test(value)) {
          tokens.push({ type: 'NUMBER', value, position: start });
          continue;
        }
        // Not a number, backtrack
        pos = start;
      }

      // Operators
      const twoCharOps = ['>=', '<=', '!=', '<>'];
      const twoChar = input.slice(pos, pos + 2);
      if (twoCharOps.includes(twoChar)) {
        tokens.push({ type: 'OPERATOR', value: twoChar === '<>' ? '!=' : twoChar, position: start });
        pos += 2;
        continue;
      }

      const oneCharOps = ['=', '>', '<', '+', '-', '*', '/', '(', ')', ',', '.'];
      if (oneCharOps.includes(input[pos])) {
        tokens.push({ type: 'OPERATOR', value: input[pos], position: start });
        pos++;
        continue;
      }

      // Identifier or keyword
      if (/[a-zA-Z_]/.test(input[pos])) {
        let value = '';
        while (pos < input.length && /[a-zA-Z0-9_.]/.test(input[pos])) {
          value += input[pos];
          pos++;
        }
        tokens.push({ type: 'IDENTIFIER', value, position: start });
        continue;
      }

      throw new QuerySyntaxError(`Unexpected character: ${input[pos]}`, pos);
    }

    return tokens;
  }

  // ============================================================================
  // Token Helpers
  // ============================================================================

  private peek(): Token {
    if (this.position >= this.tokens.length) {
      throw new QuerySyntaxError('Unexpected end of query');
    }
    return this.tokens[this.position];
  }

  private advance(): Token {
    return this.tokens[this.position++];
  }

  private match(value: string): boolean {
    if (this.position >= this.tokens.length) return false;
    if (this.tokens[this.position].value === value) {
      this.position++;
      return true;
    }
    return false;
  }

  private matchKeyword(keyword: string): boolean {
    if (this.position >= this.tokens.length) return false;
    if (this.tokens[this.position].value.toUpperCase() === keyword) {
      this.position++;
      return true;
    }
    return false;
  }

  private consume(expected: string): void {
    const token = this.peek();
    if (token.value.toUpperCase() !== expected.toUpperCase() && token.value !== expected) {
      throw new QuerySyntaxError(
        `Expected '${expected}' but found '${token.value}'`,
        token.position,
        `Try adding '${expected}' at position ${token.position}`
      );
    }
    this.advance();
  }

  private consumeIdentifier(): string {
    const token = this.peek();
    if (token.type !== 'IDENTIFIER') {
      throw new QuerySyntaxError(
        `Expected identifier but found '${token.value}'`,
        token.position
      );
    }
    this.advance();
    return token.value;
  }

  private consumeFieldExpression(): string {
    let expr = '';
    const token = this.peek();

    // Check for aggregation function
    const aggFunctions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST', 'ARRAY_AGG', 'STRING_AGG'];
    if (token.type === 'IDENTIFIER' && aggFunctions.includes(token.value.toUpperCase())) {
      expr = this.advance().value;
      if (this.match('(')) {
        if (this.match('*')) {
          expr += '(*)';
        } else {
          expr += '(' + this.consumeIdentifier() + ')';
        }
        this.consume(')');
      }

      // Check for AS alias
      if (this.matchKeyword('AS')) {
        expr += ' AS ' + this.consumeIdentifier();
      }

      return expr;
    }

    return this.consumeIdentifier();
  }

  private consumeNumber(): number {
    const token = this.peek();
    if (token.type !== 'NUMBER') {
      throw new QuerySyntaxError(
        `Expected number but found '${token.value}'`,
        token.position
      );
    }
    this.advance();
    return parseFloat(token.value);
  }

  private consumeValue(): any {
    const token = this.peek();

    if (token.type === 'STRING') {
      this.advance();
      return token.value;
    }

    if (token.type === 'NUMBER') {
      this.advance();
      return parseFloat(token.value);
    }

    if (token.value.toUpperCase() === 'TRUE') {
      this.advance();
      return true;
    }

    if (token.value.toUpperCase() === 'FALSE') {
      this.advance();
      return false;
    }

    if (token.value.toUpperCase() === 'NULL') {
      this.advance();
      return null;
    }

    // Treat as string if identifier
    if (token.type === 'IDENTIFIER') {
      this.advance();
      return token.value;
    }

    throw new QuerySyntaxError(
      `Expected value but found '${token.value}'`,
      token.position
    );
  }

  private consumeOperator(): ComparisonOperator {
    const token = this.peek();
    const operators: ComparisonOperator[] = ['=', '!=', '>', '>=', '<', '<='];

    if (operators.includes(token.value as ComparisonOperator)) {
      this.advance();
      return token.value as ComparisonOperator;
    }

    throw new QuerySyntaxError(
      `Expected comparison operator but found '${token.value}'`,
      token.position,
      'Valid operators: =, !=, >, >=, <, <='
    );
  }
}
