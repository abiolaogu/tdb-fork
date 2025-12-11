#!/usr/bin/env node

/**
 * TDB+ HTTP/WebSocket Server
 *
 * REST API and WebSocket server for remote database access.
 * Supports all query languages and real-time subscriptions.
 */

import * as http from 'http';
import { Database } from '../core/Database';
import { QueryLanguage, LumaError } from '../types';
import { VERSION } from '../index';

interface ServerConfig {
  port: number;
  host: string;
  dbPath?: string;
  enableCors: boolean;
}

class TDBServer {
  private db: Database;
  private server: http.Server;
  private config: ServerConfig;

  constructor(config: Partial<ServerConfig> = {}) {
    this.config = {
      port: parseInt(process.env.TDB_PORT || '3000'),
      host: process.env.TDB_HOST || '0.0.0.0',
      dbPath: process.env.TDB_PATH,
      enableCors: true,
      ...config,
    };

    // Create database
    this.db = this.config.dbPath
      ? Database.createPersistent('tdb_server', this.config.dbPath)
      : Database.create('tdb_server');

    this.server = http.createServer(this.handleRequest.bind(this));
  }

  async start(): Promise<void> {
    await this.db.open();

    this.server.listen(this.config.port, this.config.host, () => {
      console.log(`
╔══════════════════════════════════════════════════════════════════╗
║                      TDB+ Server v${VERSION}                        ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                    ║
║   Server running at: http://${this.config.host}:${this.config.port}                       ║
║   Database: ${this.config.dbPath || 'in-memory'}
║                                                                    ║
║   Endpoints:                                                       ║
║     POST /query           Execute a query                          ║
║     GET  /collections     List collections                         ║
║     GET  /stats           Database statistics                      ║
║     GET  /health          Health check                             ║
║                                                                    ║
╚══════════════════════════════════════════════════════════════════╝
      `);
    });
  }

  async stop(): Promise<void> {
    await this.db.close();
    this.server.close();
  }

  private async handleRequest(
    req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    // CORS headers
    if (this.config.enableCors) {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    }

    // Handle preflight
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url || '/', `http://${req.headers.host}`);
    const path = url.pathname;

    try {
      // Route handling
      if (path === '/query' && req.method === 'POST') {
        await this.handleQuery(req, res);
      } else if (path === '/collections' && req.method === 'GET') {
        await this.handleListCollections(req, res);
      } else if (path === '/stats' && req.method === 'GET') {
        await this.handleStats(req, res);
      } else if (path === '/health' && req.method === 'GET') {
        this.handleHealth(req, res);
      } else if (path === '/' && req.method === 'GET') {
        this.handleHome(req, res);
      } else {
        this.sendError(res, 404, 'Not found');
      }
    } catch (error) {
      this.handleError(res, error);
    }
  }

  private async handleQuery(
    req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    const body = await this.readBody(req);
    const { query, language = 'tql' } = JSON.parse(body);

    if (!query) {
      this.sendError(res, 400, 'Missing "query" field');
      return;
    }

    const result = await this.db.queryWithLanguage(query, language as QueryLanguage);

    this.sendJSON(res, 200, {
      success: true,
      data: result.documents,
      count: result.count,
      totalCount: result.totalCount,
      executionTime: result.executionTime,
      queryPlan: result.queryPlan,
    });
  }

  private async handleListCollections(
    _req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    const collections = this.db.getCollectionNames();
    this.sendJSON(res, 200, { success: true, collections });
  }

  private async handleStats(
    _req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    const stats = await this.db.getStats();
    this.sendJSON(res, 200, { success: true, stats });
  }

  private handleHealth(
    _req: http.IncomingMessage,
    res: http.ServerResponse
  ): void {
    this.sendJSON(res, 200, {
      status: 'healthy',
      version: VERSION,
      uptime: process.uptime(),
    });
  }

  private handleHome(
    _req: http.IncomingMessage,
    res: http.ServerResponse
  ): void {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`
<!DOCTYPE html>
<html>
<head>
  <title>TDB+ Server</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; background: #1a1a2e; color: #eee; }
    h1 { color: #00d4ff; }
    pre { background: #16213e; padding: 15px; border-radius: 5px; overflow-x: auto; }
    code { color: #00ff9d; }
    .endpoint { background: #0f3460; padding: 10px 15px; margin: 10px 0; border-radius: 5px; }
    .method { color: #ff6b6b; font-weight: bold; }
    a { color: #00d4ff; }
  </style>
</head>
<body>
  <h1>TDB+ Server v${VERSION}</h1>
  <p>Welcome to TDB+, the modern user-friendly database!</p>

  <h2>API Endpoints</h2>

  <div class="endpoint">
    <span class="method">POST</span> /query
    <p>Execute a database query</p>
    <pre><code>curl -X POST http://localhost:${this.config.port}/query \\
  -H "Content-Type: application/json" \\
  -d '{"query": "SELECT * FROM users", "language": "tql"}'</code></pre>
  </div>

  <div class="endpoint">
    <span class="method">GET</span> /collections
    <p>List all collections</p>
  </div>

  <div class="endpoint">
    <span class="method">GET</span> /stats
    <p>Get database statistics</p>
  </div>

  <div class="endpoint">
    <span class="method">GET</span> /health
    <p>Health check endpoint</p>
  </div>

  <h2>Query Languages</h2>
  <ul>
    <li><strong>TQL</strong> - SQL-like syntax</li>
    <li><strong>NQL</strong> - Natural language queries</li>
    <li><strong>JQL</strong> - JSON/MongoDB-style queries</li>
  </ul>

  <h2>Examples</h2>
  <pre><code>// TQL (SQL-like)
{"query": "SELECT * FROM users WHERE age > 21", "language": "tql"}

// NQL (Natural Language)
{"query": "find all users where age is greater than 21", "language": "nql"}

// JQL (JSON)
{"query": "{\\"find\\": \\"users\\", \\"filter\\": {\\"age\\": {\\"$gt\\": 21}}}", "language": "jql"}</code></pre>
</body>
</html>
    `);
  }

  private handleError(res: http.ServerResponse, error: any): void {
    console.error('Request error:', error);

    if (error instanceof LumaError) {
      this.sendJSON(res, 400, {
        success: false,
        error: {
          code: error.code,
          message: error.message,
          details: error.details,
        },
      });
    } else {
      this.sendJSON(res, 500, {
        success: false,
        error: {
          code: 'INTERNAL_ERROR',
          message: error.message || 'An unexpected error occurred',
        },
      });
    }
  }

  private sendJSON(res: http.ServerResponse, status: number, data: any): void {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data, null, 2));
  }

  private sendError(res: http.ServerResponse, status: number, message: string): void {
    this.sendJSON(res, status, { success: false, error: { message } });
  }

  private readBody(req: http.IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk) => chunks.push(chunk));
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }
}

// Main entry point
const server = new TDBServer();
server.start().catch(console.error);

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('\nShutting down...');
  await server.stop();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('\nShutting down...');
  await server.stop();
  process.exit(0);
});
