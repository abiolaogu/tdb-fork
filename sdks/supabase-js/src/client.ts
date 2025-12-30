/**
 * SupabaseClient - Main client for Supabase-compatible API
 */

import { PostgrestClient, PostgrestQueryBuilder } from './postgrest';
import { SupabaseAuthClient } from './auth';
import type { SupabaseClientOptions } from './types';

/**
 * Create a new Supabase client
 *
 * @example
 * ```typescript
 * const supabase = createClient('http://localhost:3000', 'your-anon-key');
 *
 * // Query database
 * const { data, error } = await supabase
 *   .from('posts')
 *   .select('*')
 *   .eq('published', true);
 *
 * // Auth
 * const { data: user, error } = await supabase.auth.signUp({
 *   email: 'user@example.com',
 *   password: 'password123'
 * });
 * ```
 */
export function createClient(
    supabaseUrl: string,
    supabaseKey: string,
    options?: SupabaseClientOptions
): SupabaseClient {
    return new SupabaseClient(supabaseUrl, supabaseKey, options);
}

/**
 * Supabase Client - Main entry point for all Supabase operations
 */
export class SupabaseClient {
    protected supabaseUrl: string;
    protected supabaseKey: string;
    protected options: SupabaseClientOptions;

    protected _auth: SupabaseAuthClient;
    protected _postgrest: PostgrestClient;

    constructor(
        supabaseUrl: string,
        supabaseKey: string,
        options?: SupabaseClientOptions
    ) {
        this.supabaseUrl = supabaseUrl.replace(/\/$/, ''); // Remove trailing slash
        this.supabaseKey = supabaseKey;
        this.options = options ?? {};

        const headers = {
            apikey: supabaseKey,
            Authorization: `Bearer ${supabaseKey}`,
            ...this.options.global?.headers,
        };

        // Initialize auth client
        const authUrl = `${this.supabaseUrl}/auth/v1`;
        this._auth = new SupabaseAuthClient(authUrl, {
            headers,
            fetch: this.options.global?.fetch,
            autoRefreshToken: this.options.auth?.autoRefreshToken,
            persistSession: this.options.auth?.persistSession,
            storage: this.options.auth?.storage,
            storageKey: this.options.auth?.storageKey,
        });

        // Initialize PostgrestClient
        const restUrl = `${this.supabaseUrl}/rest/v1`;
        this._postgrest = new PostgrestClient(restUrl, {
            headers,
            schema: this.options.db?.schema,
            fetch: this.options.global?.fetch,
        });
    }

    /**
     * Access to auth operations
     */
    get auth(): SupabaseAuthClient {
        return this._auth;
    }

    /**
     * Start a query on a table
     *
     * @example
     * ```typescript
     * const { data, error } = await supabase
     *   .from('posts')
     *   .select('id, title, author:users(name)')
     *   .eq('published', true)
     *   .order('created_at', { ascending: false })
     *   .limit(10);
     * ```
     */
    from<T = unknown>(table: string): PostgrestQueryBuilder<T> {
        // Update authorization header with current session token if available
        return this._postgrest.from<T>(table);
    }

    /**
     * Call a stored function (RPC)
     *
     * @example
     * ```typescript
     * const { data, error } = await supabase.rpc('get_user_stats', {
     *   user_id: '123'
     * });
     * ```
     */
    rpc<T = unknown>(
        fn: string,
        args?: Record<string, unknown>,
        options?: { head?: boolean; count?: 'exact' | 'planned' | 'estimated' }
    ) {
        return this._postgrest.rpc<T>(fn, args, options);
    }

    /**
     * Access to storage operations (stub for Phase 2)
     */
    get storage() {
        return {
            from: (bucket: string) => ({
                upload: async (path: string, file: File | Blob) => {
                    throw new Error('Storage not implemented yet');
                },
                download: async (path: string) => {
                    throw new Error('Storage not implemented yet');
                },
                remove: async (paths: string[]) => {
                    throw new Error('Storage not implemented yet');
                },
                list: async (path?: string) => {
                    throw new Error('Storage not implemented yet');
                },
                getPublicUrl: (path: string) => {
                    return { data: { publicUrl: `${this.supabaseUrl}/storage/v1/object/public/${bucket}/${path}` } };
                },
            }),
            createBucket: async (id: string, options?: Record<string, unknown>) => {
                throw new Error('Storage not implemented yet');
            },
            getBucket: async (id: string) => {
                throw new Error('Storage not implemented yet');
            },
            listBuckets: async () => {
                throw new Error('Storage not implemented yet');
            },
            deleteBucket: async (id: string) => {
                throw new Error('Storage not implemented yet');
            },
        };
    }

    /**
     * Access to realtime operations (stub for Phase 2)
     */
    channel(name: string) {
        return {
            on: (event: string, callback: (payload: unknown) => void) => ({
                subscribe: () => {
                    console.warn('Realtime not implemented yet');
                    return { unsubscribe: () => { } };
                },
            }),
            subscribe: () => {
                console.warn('Realtime not implemented yet');
                return { unsubscribe: () => { } };
            },
            unsubscribe: () => { },
        };
    }

    /**
     * Access to edge functions (stub for Phase 2)
     */
    get functions() {
        return {
            invoke: async <T = unknown>(
                functionName: string,
                options?: { body?: unknown; headers?: Record<string, string> }
            ): Promise<{ data: T | null; error: Error | null }> => {
                try {
                    const response = await (this.options.global?.fetch ?? globalThis.fetch)(
                        `${this.supabaseUrl}/functions/v1/${functionName}`,
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                apikey: this.supabaseKey,
                                Authorization: `Bearer ${this.supabaseKey}`,
                                ...options?.headers,
                            },
                            body: options?.body ? JSON.stringify(options.body) : undefined,
                        }
                    );

                    if (!response.ok) {
                        return {
                            data: null,
                            error: new Error(`Function invocation failed: ${response.statusText}`),
                        };
                    }

                    const data = await response.json();
                    return { data, error: null };
                } catch (error) {
                    return {
                        data: null,
                        error: error instanceof Error ? error : new Error('Unknown error'),
                    };
                }
            },
        };
    }
}
