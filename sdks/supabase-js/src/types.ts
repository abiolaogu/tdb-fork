/**
 * Core type definitions for Supabase compatibility
 */

// ============================================================================
// Client Options
// ============================================================================

export interface SupabaseClientOptions {
    auth?: {
        autoRefreshToken?: boolean;
        persistSession?: boolean;
        detectSessionInUrl?: boolean;
        storage?: Storage;
        storageKey?: string;
    };
    global?: {
        headers?: Record<string, string>;
        fetch?: typeof fetch;
    };
    db?: {
        schema?: string;
    };
    realtime?: {
        params?: Record<string, string>;
    };
}

// ============================================================================
// Auth Types
// ============================================================================

export interface User {
    id: string;
    aud: string;
    role: string;
    email?: string;
    email_confirmed_at?: string;
    phone?: string;
    phone_confirmed_at?: string;
    confirmation_sent_at?: string;
    confirmed_at?: string;
    recovery_sent_at?: string;
    last_sign_in_at?: string;
    app_metadata: Record<string, unknown>;
    user_metadata: Record<string, unknown>;
    identities?: UserIdentity[];
    created_at: string;
    updated_at: string;
}

export interface UserIdentity {
    id: string;
    user_id: string;
    identity_data: Record<string, unknown>;
    provider: string;
    last_sign_in_at?: string;
    created_at: string;
    updated_at: string;
}

export interface Session {
    access_token: string;
    token_type: string;
    expires_in: number;
    expires_at: number;
    refresh_token: string;
    user: User;
}

export interface AuthResponse {
    data: {
        user: User | null;
        session: Session | null;
    };
    error: AuthError | null;
}

export interface AuthError {
    message: string;
    status: number;
    code?: string;
}

export interface SignUpCredentials {
    email: string;
    password: string;
    options?: {
        data?: Record<string, unknown>;
        emailRedirectTo?: string;
        captchaToken?: string;
    };
}

export interface SignInCredentials {
    email: string;
    password: string;
}

export interface SignInWithOAuthCredentials {
    provider: 'google' | 'github' | 'gitlab' | 'discord' | string;
    options?: {
        redirectTo?: string;
        scopes?: string;
        queryParams?: Record<string, string>;
    };
}

// ============================================================================
// Database Types
// ============================================================================

export interface PostgrestResponse<T> {
    data: T[] | null;
    error: PostgrestError | null;
    count: number | null;
    status: number;
    statusText: string;
}

export interface PostgrestSingleResponse<T> {
    data: T | null;
    error: PostgrestError | null;
    count: number | null;
    status: number;
    statusText: string;
}

export interface PostgrestError {
    message: string;
    details: string | null;
    hint: string | null;
    code: string;
}

export interface PostgrestMaybeSingleResponse<T> {
    data: T | null;
    error: PostgrestError | null;
    count: number | null;
    status: number;
    statusText: string;
}

// ============================================================================
// Filter Types
// ============================================================================

export type FilterOperator =
    | 'eq'
    | 'neq'
    | 'gt'
    | 'gte'
    | 'lt'
    | 'lte'
    | 'like'
    | 'ilike'
    | 'is'
    | 'in'
    | 'cs'
    | 'cd'
    | 'sl'
    | 'sr'
    | 'nxl'
    | 'nxr'
    | 'adj'
    | 'ov'
    | 'fts'
    | 'plfts'
    | 'phfts'
    | 'wfts';

// ============================================================================
// Storage Types (for future use)
// ============================================================================

export interface StorageError {
    message: string;
    statusCode: string;
}

export interface FileObject {
    name: string;
    bucket_id: string;
    owner: string;
    id: string;
    updated_at: string;
    created_at: string;
    last_accessed_at: string;
    metadata: Record<string, unknown>;
}

export interface Bucket {
    id: string;
    name: string;
    owner: string;
    public: boolean;
    created_at: string;
    updated_at: string;
    file_size_limit: number | null;
    allowed_mime_types: string[] | null;
}

// ============================================================================
// Realtime Types (for future use)
// ============================================================================

export type RealtimePostgresChangesPayload<T> = {
    commit_timestamp: string;
    eventType: 'INSERT' | 'UPDATE' | 'DELETE';
    new: T;
    old: Partial<T>;
    errors: string[] | null;
    table: string;
    schema: string;
};

export type RealtimePresencePayload<T> = {
    key: string;
    currentPresences: T[];
    newPresences: T[];
    leftPresences: T[];
};
