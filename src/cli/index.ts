#!/usr/bin/env node

/**
 * LumaDB Command Line Interface
 *
 * Interactive REPL with support for multiple query languages,
 * syntax highlighting, auto-completion, and helpful error messages.
 */

import * as readline from 'readline';
import { Database } from '../core/Database';
import { QueryLanguage, LumaError, QuerySyntaxError } from '../types';
import { VERSION, CODENAME } from '../index';

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  bgBlue: '\x1b[44m',
};

class LumaCLI {
  private db: Database;
  private rl: readline.Interface;
  private currentLanguage: QueryLanguage;
  private historyFile: string[];
  private multilineMode: boolean;
  private multilineBuffer: string;

  constructor() {
    this.db = Database.create('luma_cli');
    this.currentLanguage = 'lql';
    this.historyFile = [];
    this.multilineMode = false;
    this.multilineBuffer = '';

    this.rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      completer: this.completer.bind(this),
    });
  }

  async start(): Promise<void> {
    this.printBanner();
    await this.db.open();

    this.rl.setPrompt(this.getPrompt());
    this.rl.prompt();

    this.rl.on('line', async (line) => {
      await this.handleInput(line);
      this.rl.prompt();
    });

    this.rl.on('close', async () => {
      console.log('\nGoodbye!');
      await this.db.close();
      process.exit(0);
    });
  }

  private printBanner(): void {
    console.log(`
${colors.cyan}╔══════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   ${colors.bright}██╗      ██╗   ██╗███╗   ███╗ █████╗ ██████╗ ██████╗          ${colors.reset}${colors.cyan}║
║   ${colors.bright}██║      ██║   ██║████╗ ████║██╔══██╗██╔══██╗██╔══██╗         ${colors.reset}${colors.cyan}║
║   ${colors.bright}██║      ██║   ██║██╔████╔██║███████║██║  ██║██████╔╝         ${colors.reset}${colors.cyan}║
║   ${colors.bright}██║      ██║   ██║██║╚██╔╝██║██╔══██║██║  ██║██╔══██╗         ${colors.reset}${colors.cyan}║
║   ${colors.bright}███████╗╚██████╔╝██║ ╚═╝ ██║██║  ██║██████╔╝██████╔╝         ${colors.reset}${colors.cyan}║
║   ${colors.bright}╚══════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═╝╚═════╝ ╚═════╝          ${colors.reset}${colors.cyan}║
║                                                                    ║
║   ${colors.white}Version ${VERSION} "${CODENAME}"${colors.cyan}                                        ║
║   ${colors.dim}The Modern, User-Friendly Database${colors.reset}${colors.cyan}                          ║
║                                                                    ║
║   ${colors.bright}L U M A   D A T A B A S E${colors.reset}${colors.cyan}                                    ║
║                                                                    ║
╚══════════════════════════════════════════════════════════════════╝${colors.reset}

${colors.green}Welcome to LumaDB!${colors.reset} Type ${colors.yellow}.help${colors.reset} for commands or start querying.

${colors.dim}Query Languages:${colors.reset}
  ${colors.cyan}LQL${colors.reset} - SQL-like:    SELECT * FROM users WHERE age > 21
  ${colors.cyan}NQL${colors.reset} - Natural:     find all users where age is greater than 21
  ${colors.cyan}JQL${colors.reset} - JSON:        { "find": "users", "filter": { "age": { "$gt": 21 } } }

Current language: ${colors.bright}${this.currentLanguage.toUpperCase()}${colors.reset}
`);
  }

  private getPrompt(): string {
    const langColor = {
      lql: colors.blue,
      nql: colors.green,
      jql: colors.magenta,
    }[this.currentLanguage];

    if (this.multilineMode) {
      return `${colors.dim}...${colors.reset} `;
    }

    return `${langColor}${this.currentLanguage.toUpperCase()}${colors.reset} > `;
  }

  private async handleInput(line: string): Promise<void> {
    const trimmed = line.trim();

    // Handle multiline mode
    if (this.multilineMode) {
      if (trimmed === ';' || trimmed === '') {
        const query = this.multilineBuffer.trim();
        this.multilineBuffer = '';
        this.multilineMode = false;
        this.rl.setPrompt(this.getPrompt());
        if (query) {
          await this.executeQuery(query);
        }
        return;
      }
      this.multilineBuffer += ' ' + line;
      return;
    }

    // Empty line
    if (!trimmed) {
      return;
    }

    // Commands start with .
    if (trimmed.startsWith('.')) {
      await this.handleCommand(trimmed);
      return;
    }

    // Check for multiline start (ends with \)
    if (trimmed.endsWith('\\')) {
      this.multilineMode = true;
      this.multilineBuffer = trimmed.slice(0, -1);
      this.rl.setPrompt(this.getPrompt());
      return;
    }

    // Execute query
    await this.executeQuery(trimmed);
  }

  private async handleCommand(input: string): Promise<void> {
    const parts = input.slice(1).split(/\s+/);
    const command = parts[0].toLowerCase();
    const args = parts.slice(1);

    switch (command) {
      case 'help':
      case 'h':
      case '?':
        this.printHelp();
        break;

      case 'lang':
      case 'language':
        if (args[0]) {
          const lang = args[0].toLowerCase() as QueryLanguage;
          if (['lql', 'nql', 'jql'].includes(lang)) {
            this.currentLanguage = lang;
            this.rl.setPrompt(this.getPrompt());
            console.log(`${colors.green}Switched to ${lang.toUpperCase()}${colors.reset}`);
          } else {
            console.log(`${colors.red}Unknown language: ${args[0]}. Use: lql, nql, or jql${colors.reset}`);
          }
        } else {
          console.log(`Current language: ${colors.bright}${this.currentLanguage.toUpperCase()}${colors.reset}`);
        }
        break;

      case 'lql':
        this.currentLanguage = 'lql';
        this.rl.setPrompt(this.getPrompt());
        console.log(`${colors.green}Switched to LQL (SQL-like)${colors.reset}`);
        break;

      case 'nql':
        this.currentLanguage = 'nql';
        this.rl.setPrompt(this.getPrompt());
        console.log(`${colors.green}Switched to NQL (Natural Language)${colors.reset}`);
        break;

      case 'jql':
        this.currentLanguage = 'jql';
        this.rl.setPrompt(this.getPrompt());
        console.log(`${colors.green}Switched to JQL (JSON)${colors.reset}`);
        break;

      case 'collections':
      case 'tables':
        const collections = this.db.getCollectionNames();
        if (collections.length === 0) {
          console.log(`${colors.dim}No collections yet${colors.reset}`);
        } else {
          console.log(`${colors.bright}Collections:${colors.reset}`);
          for (const name of collections) {
            console.log(`  - ${colors.cyan}${name}${colors.reset}`);
          }
        }
        break;

      case 'stats':
        const stats = await this.db.getStats();
        console.log(`${colors.bright}Database Statistics:${colors.reset}`);
        console.log(`  Total Documents: ${colors.cyan}${stats.totalDocuments}${colors.reset}`);
        console.log(`  Total Size: ${colors.cyan}${this.formatBytes(stats.totalSize)}${colors.reset}`);
        console.log(`  Collections: ${colors.cyan}${stats.collections.length}${colors.reset}`);
        if (stats.collections.length > 0) {
          console.log(`\n${colors.bright}Collection Details:${colors.reset}`);
          for (const col of stats.collections) {
            console.log(`  ${colors.cyan}${col.name}${colors.reset}: ${col.documentCount} docs, ${this.formatBytes(col.size)}`);
          }
        }
        break;

      case 'clear':
      case 'cls':
        console.clear();
        this.printBanner();
        break;

      case 'exit':
      case 'quit':
      case 'q':
        this.rl.close();
        break;

      case 'examples':
        this.printExamples();
        break;

      case 'tutorial':
        this.printTutorial();
        break;

      default:
        console.log(`${colors.red}Unknown command: .${command}${colors.reset}`);
        console.log(`Type ${colors.yellow}.help${colors.reset} for available commands`);
    }
  }

  private async executeQuery(query: string): Promise<void> {
    try {
      const startTime = Date.now();
      const result = await this.db.queryWithLanguage(query, this.currentLanguage);
      const elapsed = Date.now() - startTime;

      // Display results
      if (result.documents.length === 0) {
        console.log(`${colors.dim}No results${colors.reset}`);
      } else {
        this.displayResults(result.documents);
      }

      // Display stats
      console.log(
        `${colors.dim}${result.count} row(s) returned in ${elapsed}ms${colors.reset}`
      );

      // Show query plan if available
      if (result.queryPlan) {
        console.log(`\n${colors.bright}Query Plan:${colors.reset}`);
        for (const step of result.queryPlan.steps) {
          console.log(`  ${colors.cyan}${step.operation}${colors.reset}: ${step.description}`);
        }
        console.log(`  ${colors.dim}Estimated cost: ${result.queryPlan.estimatedCost}${colors.reset}`);
      }
    } catch (error) {
      this.handleError(error);
    }
  }

  private displayResults(documents: any[]): void {
    if (documents.length === 0) return;

    // Get all keys
    const allKeys = new Set<string>();
    for (const doc of documents) {
      Object.keys(doc).forEach((k) => allKeys.add(k));
    }
    const keys = Array.from(allKeys);

    // Calculate column widths
    const widths: Record<string, number> = {};
    for (const key of keys) {
      widths[key] = Math.max(key.length, 10);
      for (const doc of documents) {
        const value = this.formatValue(doc[key]);
        widths[key] = Math.min(Math.max(widths[key], value.length), 30);
      }
    }

    // Print header
    const header = keys.map((k) => this.padRight(k, widths[k])).join(' | ');
    const separator = keys.map((k) => '-'.repeat(widths[k])).join('-+-');

    console.log(`${colors.bright}${header}${colors.reset}`);
    console.log(separator);

    // Print rows
    for (const doc of documents.slice(0, 100)) {
      const row = keys
        .map((k) => this.padRight(this.formatValue(doc[k]), widths[k]))
        .join(' | ');
      console.log(row);
    }

    if (documents.length > 100) {
      console.log(`${colors.dim}... and ${documents.length - 100} more rows${colors.reset}`);
    }
  }

  private handleError(error: any): void {
    if (error instanceof QuerySyntaxError) {
      console.log(`\n${colors.red}Syntax Error: ${error.message}${colors.reset}`);
      if (error.suggestion) {
        console.log(`${colors.yellow}Suggestion: ${error.suggestion}${colors.reset}`);
      }
      if (error.position !== undefined) {
        console.log(`${colors.dim}At position: ${error.position}${colors.reset}`);
      }
    } else if (error instanceof LumaError) {
      console.log(`\n${colors.red}Error (${error.code}): ${error.message}${colors.reset}`);
    } else {
      console.log(`\n${colors.red}Error: ${error.message || error}${colors.reset}`);
    }
  }

  private printHelp(): void {
    console.log(`
${colors.bright}LumaDB Commands:${colors.reset}

  ${colors.yellow}.help, .h, .?${colors.reset}      Show this help message
  ${colors.yellow}.lang <language>${colors.reset}  Switch query language (lql, nql, jql)
  ${colors.yellow}.lql${colors.reset}               Switch to LQL (SQL-like)
  ${colors.yellow}.nql${colors.reset}               Switch to NQL (Natural Language)
  ${colors.yellow}.jql${colors.reset}               Switch to JQL (JSON)
  ${colors.yellow}.collections${colors.reset}       List all collections
  ${colors.yellow}.stats${colors.reset}             Show database statistics
  ${colors.yellow}.examples${colors.reset}          Show query examples
  ${colors.yellow}.tutorial${colors.reset}          Interactive tutorial
  ${colors.yellow}.clear${colors.reset}             Clear the screen
  ${colors.yellow}.exit, .quit${colors.reset}       Exit LumaDB

${colors.bright}Query Tips:${colors.reset}
  - End a line with \\ for multi-line queries
  - Use ; or empty line to execute multi-line query
  - Press Ctrl+C to cancel current query
  - Press Ctrl+D or type .exit to quit
`);
  }

  private printExamples(): void {
    console.log(`
${colors.bright}LQL Examples (SQL-like):${colors.reset}
  SELECT * FROM users
  SELECT name, email FROM users WHERE age > 21 ORDER BY name
  INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 25)
  UPDATE users SET status = 'active' WHERE email = 'john@example.com'
  DELETE FROM users WHERE inactive = true
  SELECT department, COUNT(*) FROM employees GROUP BY department

${colors.bright}NQL Examples (Natural Language):${colors.reset}
  find all users
  get users where age is greater than 21
  show first 10 products sorted by price descending
  count orders where status equals "pending"
  find users where name contains "John"
  update users set status to "active" where verified is true
  remove users where inactive is true

${colors.bright}JQL Examples (JSON):${colors.reset}
  { "find": "users" }
  { "find": "users", "filter": { "age": { "$gt": 21 } }, "limit": 10 }
  { "insert": "users", "documents": [{ "name": "John", "age": 25 }] }
  { "update": "users", "filter": { "id": "123" }, "set": { "status": "active" } }
  { "delete": "users", "filter": { "inactive": true } }
  { "count": "users", "filter": { "status": "active" } }
`);
  }

  private printTutorial(): void {
    console.log(`
${colors.bright}Welcome to the LumaDB Tutorial!${colors.reset}

Let's learn the basics of LumaDB step by step.

${colors.cyan}Step 1: Create your first collection${colors.reset}
  LQL: INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 28)
  NQL: add to users name "Alice", email "alice@example.com", age 28
  JQL: { "insert": "users", "documents": [{ "name": "Alice", "email": "alice@example.com", "age": 28 }] }

${colors.cyan}Step 2: Query your data${colors.reset}
  LQL: SELECT * FROM users
  NQL: find all users
  JQL: { "find": "users" }

${colors.cyan}Step 3: Filter results${colors.reset}
  LQL: SELECT * FROM users WHERE age > 25
  NQL: get users where age is greater than 25
  JQL: { "find": "users", "filter": { "age": { "$gt": 25 } } }

${colors.cyan}Step 4: Update documents${colors.reset}
  LQL: UPDATE users SET age = 29 WHERE name = 'Alice'
  NQL: update users set age to 29 where name equals "Alice"
  JQL: { "update": "users", "filter": { "name": "Alice" }, "set": { "age": 29 } }

${colors.cyan}Step 5: Delete documents${colors.reset}
  LQL: DELETE FROM users WHERE name = 'Alice'
  NQL: remove users where name equals "Alice"
  JQL: { "delete": "users", "filter": { "name": "Alice" } }

${colors.dim}Type .examples for more query examples${colors.reset}
`);
  }

  private completer(line: string): [string[], string] {
    const keywords = [
      // LQL keywords
      'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
      'DELETE', 'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'AND', 'OR',
      'GROUP', 'HAVING', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CREATE', 'DROP',
      'INDEX', 'ON', 'UNIQUE', 'BETWEEN', 'IN', 'LIKE', 'NOT', 'NULL', 'IS',
      // NQL keywords
      'find', 'get', 'show', 'count', 'add', 'update', 'remove', 'delete',
      'all', 'where', 'with', 'sorted', 'ordered', 'by', 'ascending', 'descending',
      'equals', 'greater', 'less', 'than', 'contains', 'starts', 'ends',
      'between', 'first', 'top', 'limit', 'skip',
      // Commands
      '.help', '.lang', '.lql', '.nql', '.jql', '.collections', '.stats',
      '.examples', '.tutorial', '.clear', '.exit', '.quit',
    ];

    // Add collection names
    const collections = this.db.getCollectionNames();

    const completions = [...keywords, ...collections];
    const hits = completions.filter((c) =>
      c.toLowerCase().startsWith(line.toLowerCase())
    );

    return [hits.length ? hits : completions, line];
  }

  private formatValue(value: any): string {
    if (value === null || value === undefined) {
      return 'null';
    }
    if (typeof value === 'object') {
      if (value instanceof Date) {
        return value.toISOString().slice(0, 19);
      }
      return JSON.stringify(value);
    }
    return String(value);
  }

  private padRight(str: string, length: number): string {
    if (str.length > length) {
      return str.slice(0, length - 3) + '...';
    }
    return str + ' '.repeat(length - str.length);
  }

  private formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }
}

// Main entry point
const cli = new LumaCLI();
cli.start().catch(console.error);
