/**
 * PostgrestClient - PostgREST-compatible query builder
 */

import type {
    PostgrestResponse,
    PostgrestSingleResponse,
    PostgrestMaybeSingleResponse,
    PostgrestError,
} from './types';

/**
 * PostgrestClient for building database queries
 */
export class PostgrestClient {
    protected url: string;
    protected headers: Record<string, string>;
    protected schema: string;
    protected fetch: typeof fetch;

    constructor(
        url: string,
        options?: {
            headers?: Record<string, string>;
            schema?: string;
            fetch?: typeof fetch;
        }
    ) {
        this.url = url;
        this.headers = options?.headers ?? {};
        this.schema = options?.schema ?? 'public';
        this.fetch = options?.fetch ?? globalThis.fetch;
    }

    /**
     * Start a query on a table
     */
    from<T = unknown>(table: string): PostgrestQueryBuilder<T> {
        const url = `${this.url}/${table}`;
        return new PostgrestQueryBuilder<T>(url, {
            headers: this.headers,
            schema: this.schema,
            fetch: this.fetch,
        });
    }

    /**
     * Call a stored function (RPC)
     */
    rpc<T = unknown>(
        fn: string,
        args?: Record<string, unknown>,
        options?: { head?: boolean; count?: 'exact' | 'planned' | 'estimated' }
    ): PostgrestFilterBuilder<T> {
        const url = `${this.url}/rpc/${fn}`;
        const builder = new PostgrestFilterBuilder<T>(url, {
            headers: this.headers,
            schema: this.schema,
            fetch: this.fetch,
            method: 'POST',
            body: args,
        });
        return builder;
    }
}

/**
 * PostgrestQueryBuilder - Entry point for table queries
 */
export class PostgrestQueryBuilder<T> {
    protected url: string;
    protected headers: Record<string, string>;
    protected schema: string;
    protected fetchFn: typeof fetch;

    constructor(
        url: string,
        options: {
            headers: Record<string, string>;
            schema: string;
            fetch: typeof fetch;
        }
    ) {
        this.url = url;
        this.headers = options.headers;
        this.schema = options.schema;
        this.fetchFn = options.fetch;
    }

    /**
     * SELECT query
     */
    select(columns = '*', options?: { head?: boolean; count?: 'exact' | 'planned' | 'estimated' }): PostgrestFilterBuilder<T> {
        const searchParams = new URLSearchParams();
        searchParams.set('select', columns);

        if (options?.count) {
            this.headers['Prefer'] = `count=${options.count}`;
        }

        return new PostgrestFilterBuilder<T>(`${this.url}?${searchParams.toString()}`, {
            headers: this.headers,
            schema: this.schema,
            fetch: this.fetchFn,
            method: options?.head ? 'HEAD' : 'GET',
        });
    }

    /**
     * INSERT query
     */
    insert(values: Partial<T> | Partial<T>[], options?: { count?: 'exact' | 'planned' | 'estimated'; returning?: 'minimal' | 'representation' }): PostgrestFilterBuilder<T> {
        const headers = { ...this.headers };
        headers['Content-Type'] = 'application/json';

        if (options?.returning === 'representation') {
            headers['Prefer'] = 'return=representation';
        } else if (options?.returning === 'minimal') {
            headers['Prefer'] = 'return=minimal';
        } else {
            headers['Prefer'] = 'return=representation';
        }

        return new PostgrestFilterBuilder<T>(this.url, {
            headers,
            schema: this.schema,
            fetch: this.fetchFn,
            method: 'POST',
            body: values,
        });
    }

    /**
     * UPDATE query
     */
    update(values: Partial<T>, options?: { count?: 'exact' | 'planned' | 'estimated'; returning?: 'minimal' | 'representation' }): PostgrestFilterBuilder<T> {
        const headers = { ...this.headers };
        headers['Content-Type'] = 'application/json';

        if (options?.returning === 'representation') {
            headers['Prefer'] = 'return=representation';
        } else if (options?.returning === 'minimal') {
            headers['Prefer'] = 'return=minimal';
        } else {
            headers['Prefer'] = 'return=representation';
        }

        return new PostgrestFilterBuilder<T>(this.url, {
            headers,
            schema: this.schema,
            fetch: this.fetchFn,
            method: 'PATCH',
            body: values,
        });
    }

    /**
     * DELETE query
     */
    delete(options?: { count?: 'exact' | 'planned' | 'estimated'; returning?: 'minimal' | 'representation' }): PostgrestFilterBuilder<T> {
        const headers = { ...this.headers };

        if (options?.returning === 'representation') {
            headers['Prefer'] = 'return=representation';
        }

        return new PostgrestFilterBuilder<T>(this.url, {
            headers,
            schema: this.schema,
            fetch: this.fetchFn,
            method: 'DELETE',
        });
    }

    /**
     * UPSERT query
     */
    upsert(values: Partial<T> | Partial<T>[], options?: { onConflict?: string; ignoreDuplicates?: boolean; count?: 'exact' | 'planned' | 'estimated'; returning?: 'minimal' | 'representation' }): PostgrestFilterBuilder<T> {
        const headers = { ...this.headers };
        headers['Content-Type'] = 'application/json';

        const preference = ['return=representation'];
        if (options?.onConflict) {
            preference.push(`on_conflict=${options.onConflict}`);
        }
        if (options?.ignoreDuplicates) {
            preference.push('resolution=ignore-duplicates');
        }
        headers['Prefer'] = preference.join(',');

        return new PostgrestFilterBuilder<T>(this.url, {
            headers,
            schema: this.schema,
            fetch: this.fetchFn,
            method: 'POST',
            body: values,
        });
    }
}

/**
 * PostgrestFilterBuilder - Chainable filter methods
 */
export class PostgrestFilterBuilder<T> implements PromiseLike<PostgrestResponse<T>> {
    protected url: string;
    protected headers: Record<string, string>;
    protected schema: string;
    protected fetchFn: typeof fetch;
    protected method: string;
    protected body?: unknown;
    protected signal?: AbortSignal;

    constructor(
        url: string,
        options: {
            headers: Record<string, string>;
            schema: string;
            fetch: typeof fetch;
            method?: string;
            body?: unknown;
        }
    ) {
        this.url = url;
        this.headers = options.headers;
        this.schema = options.schema;
        this.fetchFn = options.fetch;
        this.method = options.method ?? 'GET';
        this.body = options.body;
    }

    // ========================================================================
    // Filter Methods
    // ========================================================================

    eq(column: string, value: unknown): this {
        this.appendSearchParam(column, `eq.${value}`);
        return this;
    }

    neq(column: string, value: unknown): this {
        this.appendSearchParam(column, `neq.${value}`);
        return this;
    }

    gt(column: string, value: unknown): this {
        this.appendSearchParam(column, `gt.${value}`);
        return this;
    }

    gte(column: string, value: unknown): this {
        this.appendSearchParam(column, `gte.${value}`);
        return this;
    }

    lt(column: string, value: unknown): this {
        this.appendSearchParam(column, `lt.${value}`);
        return this;
    }

    lte(column: string, value: unknown): this {
        this.appendSearchParam(column, `lte.${value}`);
        return this;
    }

    like(column: string, pattern: string): this {
        this.appendSearchParam(column, `like.${pattern}`);
        return this;
    }

    ilike(column: string, pattern: string): this {
        this.appendSearchParam(column, `ilike.${pattern}`);
        return this;
    }

    is(column: string, value: boolean | null): this {
        this.appendSearchParam(column, `is.${value}`);
        return this;
    }

    in(column: string, values: unknown[]): this {
        this.appendSearchParam(column, `in.(${values.join(',')})`);
        return this;
    }

    contains(column: string, value: unknown[]): this {
        this.appendSearchParam(column, `cs.{${value.join(',')}}`);
        return this;
    }

    containedBy(column: string, value: unknown[]): this {
        this.appendSearchParam(column, `cd.{${value.join(',')}}`);
        return this;
    }

    overlaps(column: string, value: unknown[]): this {
        this.appendSearchParam(column, `ov.{${value.join(',')}}`);
        return this;
    }

    textSearch(column: string, query: string, options?: { type?: 'plain' | 'phrase' | 'websearch'; config?: string }): this {
        const type = options?.type ?? 'plain';
        const configPart = options?.config ? `(${options.config})` : '';

        let operator: string;
        switch (type) {
            case 'plain':
                operator = 'plfts';
                break;
            case 'phrase':
                operator = 'phfts';
                break;
            case 'websearch':
                operator = 'wfts';
                break;
            default:
                operator = 'fts';
        }

        this.appendSearchParam(column, `${operator}${configPart}.${query}`);
        return this;
    }

    match(query: Record<string, unknown>): this {
        Object.entries(query).forEach(([column, value]) => {
            this.eq(column, value);
        });
        return this;
    }

    not(column: string, operator: string, value: unknown): this {
        this.appendSearchParam(column, `not.${operator}.${value}`);
        return this;
    }

    or(filters: string, options?: { foreignTable?: string }): this {
        if (options?.foreignTable) {
            this.appendSearchParam(`${options.foreignTable}.or`, `(${filters})`);
        } else {
            this.appendSearchParam('or', `(${filters})`);
        }
        return this;
    }

    filter(column: string, operator: string, value: unknown): this {
        this.appendSearchParam(column, `${operator}.${value}`);
        return this;
    }

    // ========================================================================
    // Ordering & Pagination
    // ========================================================================

    order(column: string, options?: { ascending?: boolean; nullsFirst?: boolean; foreignTable?: string }): this {
        const ascending = options?.ascending ?? true;
        const nullsFirst = options?.nullsFirst;

        let orderValue = column;
        orderValue += ascending ? '.asc' : '.desc';
        if (nullsFirst !== undefined) {
            orderValue += nullsFirst ? '.nullsfirst' : '.nullslast';
        }

        if (options?.foreignTable) {
            this.appendSearchParam(`${options.foreignTable}.order`, orderValue);
        } else {
            this.appendSearchParam('order', orderValue);
        }
        return this;
    }

    limit(count: number, options?: { foreignTable?: string }): this {
        if (options?.foreignTable) {
            this.appendSearchParam(`${options.foreignTable}.limit`, count.toString());
        } else {
            this.appendSearchParam('limit', count.toString());
        }
        return this;
    }

    range(from: number, to: number, options?: { foreignTable?: string }): this {
        if (options?.foreignTable) {
            this.appendSearchParam(`${options.foreignTable}.offset`, from.toString());
            this.appendSearchParam(`${options.foreignTable}.limit`, (to - from + 1).toString());
        } else {
            this.appendSearchParam('offset', from.toString());
            this.appendSearchParam('limit', (to - from + 1).toString());
        }
        return this;
    }

    // ========================================================================
    // Execution Modifiers
    // ========================================================================

    abortSignal(signal: AbortSignal): this {
        this.signal = signal;
        return this;
    }

    single(): PromiseLike<PostgrestSingleResponse<T>> {
        this.headers['Accept'] = 'application/vnd.pgrst.object+json';
        return this.execute().then((res) => ({
            ...res,
            data: res.data?.[0] ?? null,
        })) as any;
    }

    maybeSingle(): PromiseLike<PostgrestMaybeSingleResponse<T>> {
        this.headers['Accept'] = 'application/vnd.pgrst.object+json';
        return this.execute().then((res) => ({
            ...res,
            data: res.data?.[0] ?? null,
        })) as any;
    }

    csv(): PromiseLike<{ data: string | null; error: PostgrestError | null }> {
        this.headers['Accept'] = 'text/csv';
        return this.execute().then((res) => ({
            data: res.data as unknown as string,
            error: res.error,
        }));
    }

    // ========================================================================
    // Promise Implementation
    // ========================================================================

    then<TResult1 = PostgrestResponse<T>, TResult2 = never>(
        onfulfilled?: ((value: PostgrestResponse<T>) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
    ): PromiseLike<TResult1 | TResult2> {
        return this.execute().then(onfulfilled, onrejected);
    }

    protected async execute(): Promise<PostgrestResponse<T>> {
        try {
            const response = await this.fetchFn(this.url, {
                method: this.method,
                headers: this.headers,
                body: this.body ? JSON.stringify(this.body) : undefined,
                signal: this.signal,
            });

            const contentType = response.headers.get('content-type') ?? '';
            let data: T[] | null = null;

            if (contentType.includes('application/json')) {
                const json = await response.json();
                data = Array.isArray(json) ? json : [json];
            }

            if (!response.ok) {
                const error: PostgrestError = data
                    ? (data as unknown as PostgrestError)
                    : {
                        message: response.statusText,
                        details: null,
                        hint: null,
                        code: String(response.status),
                    };

                return {
                    data: null,
                    error,
                    count: null,
                    status: response.status,
                    statusText: response.statusText,
                };
            }

            // Parse count from Content-Range header
            let count: number | null = null;
            const contentRange = response.headers.get('content-range');
            if (contentRange) {
                const match = contentRange.match(/\/(\d+|\*)/);
                if (match && match[1] !== '*') {
                    count = parseInt(match[1], 10);
                }
            }

            return {
                data,
                error: null,
                count,
                status: response.status,
                statusText: response.statusText,
            };
        } catch (error) {
            return {
                data: null,
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    details: null,
                    hint: null,
                    code: 'FETCH_ERROR',
                },
                count: null,
                status: 0,
                statusText: 'FETCH_ERROR',
            };
        }
    }

    protected appendSearchParam(key: string, value: string): void {
        const url = new URL(this.url);
        url.searchParams.append(key, value);
        this.url = url.toString();
    }
}
