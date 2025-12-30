/**
 * LumaDB D1 Compatibility - Cloudflare Worker Example
 * 
 * This example demonstrates how to use LumaDB as a drop-in replacement for D1
 * in a Cloudflare Worker environment.
 */

interface Env {
    // Binding to LumaDB (simulate D1 via fetch or binding if supported)
    DB: D1Database;
}

interface User {
    id: number;
    name: string;
    email: string;
    created_at: string;
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);

        // Route handling
        switch (url.pathname) {
            case '/init':
                return await handleInit(env.DB);
            case '/users':
                if (request.method === 'POST') {
                    return await handleCreateUser(request, env.DB);
                } else {
                    return await handleListUsers(env.DB);
                }
            case '/query':
                return await handleCustomQuery(request, env.DB);
            default:
                return new Response('Not Found', { status: 404 });
        }
    },
};

// ===== Route Handlers =====

async function handleInit(db: D1Database): Promise<Response> {
    try {
        const start = performance.now();

        // Batch execute multiple statements
        await db.batch([
            db.prepare(`
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE NOT NULL,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `),
            db.prepare(`
        INSERT INTO users (name, email) VALUES 
        ('Alice', 'alice@example.com'),
        ('Bob', 'bob@example.com')
        ON CONFLICT DO NOTHING
      `)
        ]);

        const duration = performance.now() - start;
        return Response.json({ status: 'ok', initialized: true, duration_ms: duration });
    } catch (err) {
        return handleError(err);
    }
}

async function handleCreateUser(request: Request, db: D1Database): Promise<Response> {
    try {
        const data = await request.json() as any;

        // Parameterized query for security
        const stmt = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
        const result = await stmt.bind(data.name, data.email).run();

        return Response.json({
            success: result.success,
            created: true,
            meta: result.meta
        });
    } catch (err) {
        return handleError(err);
    }
}

async function handleListUsers(db: D1Database): Promise<Response> {
    try {
        // Basic query with typed results
        const { results } = await db.prepare('SELECT * FROM users ORDER BY created_at DESC').all<User>();

        return Response.json({
            count: results.length,
            users: results
        });
    } catch (err) {
        return handleError(err);
    }
}

async function handleCustomQuery(request: Request, db: D1Database): Promise<Response> {
    try {
        const url = new URL(request.url);
        const sql = url.searchParams.get('sql');

        if (!sql) {
            return new Response('Missing SQL parameter', { status: 400 });
        }

        // Prepare statement and execute raw
        const result = await db.prepare(sql).all();
        return Response.json(result);
    } catch (err) {
        return handleError(err);
    }
}

// ===== Error Handling =====

function handleError(err: any): Response {
    console.error('Database Error:', err);

    return new Response(JSON.stringify({
        success: false,
        error: err.message || 'Internal Server Error'
    }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
    });
}

// ===== Polyfill for Local Testing =====

/**
 * If running locally outside of Workers runtime, you can use this polyfill
 * to connect to LumaDB's HTTP API.
 */
class LumaD1Polyfill {
    private endpoint: string;
    private token: string;

    constructor(endpoint: string, token: string = '') {
        this.endpoint = endpoint;
        this.token = token;
    }

    prepare(query: string): D1PreparedStatement {
        return new LumaPreparedStatement(this, query);
    }

    async batch(statements: D1PreparedStatement[]): Promise<D1Result[]> {
        // In a real implementation, this would send a batch request
        const results = [];
        for (const stmt of statements) {
            // @ts-ignore - accessing internal property
            results.push(await stmt.run());
        }
        return results;
    }

    // Internal execute method
    async _execute(sql: string, params: any[] = []): Promise<any> {
        const response = await fetch(`${this.endpoint}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.token}`
            },
            body: JSON.stringify({ sql, params })
        });

        if (!response.ok) {
            throw new Error(`LumaDB Error: ${response.statusText}`);
        }

        return await response.json();
    }
}

class LumaPreparedStatement {
    private db: LumaD1Polyfill;
    private sql: string;
    private params: any[];

    constructor(db: LumaD1Polyfill, sql: string, params: any[] = []) {
        this.db = db;
        this.sql = sql;
        this.params = params;
    }

    bind(...params: any[]): LumaPreparedStatement {
        return new LumaPreparedStatement(this.db, this.sql, params);
    }

    async first<T = unknown>(colName?: string): Promise<T | null> {
        const result = await this.all<T>();
        if (!result.results.length) return null;
        if (colName) return (result.results[0] as any)[colName];
        return result.results[0];
    }

    async run(): Promise<D1Result> {
        // Map LumaDB response to D1Result
        const raw = await this.db._execute(this.sql, this.params);
        return {
            success: true,
            meta: raw.meta || {},
            results: []
        };
    }

    async all<T = unknown>(): Promise<D1Result<T>> {
        const raw = await this.db._execute(this.sql, this.params);
        return {
            success: true,
            meta: raw.meta || {},
            results: raw.results || []
        };
    }

    async raw<T = unknown>(): Promise<T[]> {
        const result = await this.all();
        // @ts-ignore
        return result.results.map(row => Object.values(row));
    }
}
