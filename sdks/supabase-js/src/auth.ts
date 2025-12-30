/**
 * SupabaseAuthClient - Authentication client compatible with Supabase Auth
 */

import type {
    User,
    Session,
    AuthResponse,
    AuthError,
    SignUpCredentials,
    SignInCredentials,
    SignInWithOAuthCredentials,
} from './types';

/**
 * Auth client for Supabase-compatible authentication
 */
export class SupabaseAuthClient {
    protected url: string;
    protected headers: Record<string, string>;
    protected fetch: typeof fetch;
    protected currentSession: Session | null = null;
    protected autoRefreshToken: boolean;
    protected persistSession: boolean;
    protected storageKey: string;
    protected storage?: Storage;

    constructor(
        url: string,
        options?: {
            headers?: Record<string, string>;
            fetch?: typeof fetch;
            autoRefreshToken?: boolean;
            persistSession?: boolean;
            storage?: Storage;
            storageKey?: string;
        }
    ) {
        this.url = url;
        this.headers = {
            'Content-Type': 'application/json',
            ...options?.headers,
        };
        this.fetch = options?.fetch ?? globalThis.fetch;
        this.autoRefreshToken = options?.autoRefreshToken ?? true;
        this.persistSession = options?.persistSession ?? true;
        this.storageKey = options?.storageKey ?? 'supabase.auth.token';
        this.storage = options?.storage;

        // Load persisted session
        this.loadSession();
    }

    /**
     * Get current session
     */
    async getSession(): Promise<{ data: { session: Session | null }; error: AuthError | null }> {
        return {
            data: { session: this.currentSession },
            error: null,
        };
    }

    /**
     * Get current user
     */
    async getUser(): Promise<{ data: { user: User | null }; error: AuthError | null }> {
        if (!this.currentSession) {
            return { data: { user: null }, error: null };
        }

        try {
            const response = await this.fetch(`${this.url}/user`, {
                method: 'GET',
                headers: {
                    ...this.headers,
                    Authorization: `Bearer ${this.currentSession.access_token}`,
                },
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: { user: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            const user = await response.json();
            return { data: { user }, error: null };
        } catch (error) {
            return {
                data: { user: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Sign up with email and password
     */
    async signUp(credentials: SignUpCredentials): Promise<AuthResponse> {
        try {
            const response = await this.fetch(`${this.url}/signup`, {
                method: 'POST',
                headers: this.headers,
                body: JSON.stringify({
                    email: credentials.email,
                    password: credentials.password,
                    data: credentials.options?.data,
                }),
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: { user: null, session: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            const data = await response.json();
            const session: Session = {
                access_token: data.access_token,
                token_type: data.token_type || 'bearer',
                expires_in: data.expires_in,
                expires_at: data.expires_at,
                refresh_token: data.refresh_token,
                user: data.user,
            };

            this.setSession(session);

            return {
                data: { user: data.user, session },
                error: null,
            };
        } catch (error) {
            return {
                data: { user: null, session: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Sign in with email and password
     */
    async signInWithPassword(credentials: SignInCredentials): Promise<AuthResponse> {
        try {
            const response = await this.fetch(`${this.url}/token?grant_type=password`, {
                method: 'POST',
                headers: this.headers,
                body: JSON.stringify({
                    email: credentials.email,
                    password: credentials.password,
                }),
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: { user: null, session: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            const data = await response.json();
            const session: Session = {
                access_token: data.access_token,
                token_type: data.token_type || 'bearer',
                expires_in: data.expires_in,
                expires_at: data.expires_at,
                refresh_token: data.refresh_token,
                user: data.user,
            };

            this.setSession(session);

            return {
                data: { user: data.user, session },
                error: null,
            };
        } catch (error) {
            return {
                data: { user: null, session: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Sign in with OAuth provider (returns redirect URL)
     */
    async signInWithOAuth(credentials: SignInWithOAuthCredentials): Promise<{ data: { provider: string; url: string }; error: AuthError | null }> {
        const params = new URLSearchParams();
        if (credentials.options?.redirectTo) {
            params.set('redirect_to', credentials.options.redirectTo);
        }
        if (credentials.options?.scopes) {
            params.set('scopes', credentials.options.scopes);
        }

        const url = `${this.url}/authorize?provider=${credentials.provider}&${params.toString()}`;

        return {
            data: { provider: credentials.provider, url },
            error: null,
        };
    }

    /**
     * Sign in with magic link
     */
    async signInWithOtp(options: { email: string; options?: { emailRedirectTo?: string } }): Promise<{ data: { user: null; session: null }; error: AuthError | null }> {
        try {
            const response = await this.fetch(`${this.url}/magiclink`, {
                method: 'POST',
                headers: this.headers,
                body: JSON.stringify({
                    email: options.email,
                }),
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: { user: null, session: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            return {
                data: { user: null, session: null },
                error: null,
            };
        } catch (error) {
            return {
                data: { user: null, session: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Sign out
     */
    async signOut(): Promise<{ error: AuthError | null }> {
        if (!this.currentSession) {
            return { error: null };
        }

        try {
            await this.fetch(`${this.url}/logout`, {
                method: 'POST',
                headers: {
                    ...this.headers,
                    Authorization: `Bearer ${this.currentSession.access_token}`,
                },
            });

            this.clearSession();
            return { error: null };
        } catch (error) {
            this.clearSession();
            return { error: null };
        }
    }

    /**
     * Recover password (send reset email)
     */
    async resetPasswordForEmail(email: string, options?: { redirectTo?: string }): Promise<{ data: {}; error: AuthError | null }> {
        try {
            const response = await this.fetch(`${this.url}/recover`, {
                method: 'POST',
                headers: this.headers,
                body: JSON.stringify({ email }),
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: {},
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            return { data: {}, error: null };
        } catch (error) {
            return {
                data: {},
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Update user
     */
    async updateUser(attributes: { email?: string; password?: string; data?: Record<string, unknown> }): Promise<{ data: { user: User | null }; error: AuthError | null }> {
        if (!this.currentSession) {
            return {
                data: { user: null },
                error: { message: 'Not authenticated', status: 401 },
            };
        }

        try {
            const response = await this.fetch(`${this.url}/user`, {
                method: 'PUT',
                headers: {
                    ...this.headers,
                    Authorization: `Bearer ${this.currentSession.access_token}`,
                },
                body: JSON.stringify(attributes),
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    data: { user: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            const user = await response.json();
            return { data: { user }, error: null };
        } catch (error) {
            return {
                data: { user: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Refresh the access token
     */
    async refreshSession(): Promise<AuthResponse> {
        if (!this.currentSession) {
            return {
                data: { user: null, session: null },
                error: { message: 'No session to refresh', status: 400 },
            };
        }

        try {
            const response = await this.fetch(`${this.url}/token?grant_type=refresh_token`, {
                method: 'POST',
                headers: this.headers,
                body: JSON.stringify({
                    refresh_token: this.currentSession.refresh_token,
                }),
            });

            if (!response.ok) {
                const error = await response.json();
                this.clearSession();
                return {
                    data: { user: null, session: null },
                    error: {
                        message: error.message || response.statusText,
                        status: response.status,
                        code: error.code,
                    },
                };
            }

            const data = await response.json();
            const session: Session = {
                access_token: data.access_token,
                token_type: data.token_type || 'bearer',
                expires_in: data.expires_in,
                expires_at: data.expires_at,
                refresh_token: data.refresh_token,
                user: data.user,
            };

            this.setSession(session);

            return {
                data: { user: data.user, session },
                error: null,
            };
        } catch (error) {
            return {
                data: { user: null, session: null },
                error: {
                    message: error instanceof Error ? error.message : 'Unknown error',
                    status: 500,
                },
            };
        }
    }

    /**
     * Listen to auth state changes
     */
    onAuthStateChange(callback: (event: string, session: Session | null) => void): { data: { subscription: { unsubscribe: () => void } } } {
        // Simplified implementation - full implementation would use EventEmitter
        return {
            data: {
                subscription: {
                    unsubscribe: () => { },
                },
            },
        };
    }

    // ========================================================================
    // Private Methods
    // ========================================================================

    private setSession(session: Session): void {
        this.currentSession = session;
        if (this.persistSession && this.storage) {
            this.storage.setItem(this.storageKey, JSON.stringify(session));
        }
    }

    private clearSession(): void {
        this.currentSession = null;
        if (this.storage) {
            this.storage.removeItem(this.storageKey);
        }
    }

    private loadSession(): void {
        if (this.storage) {
            const stored = this.storage.getItem(this.storageKey);
            if (stored) {
                try {
                    this.currentSession = JSON.parse(stored);
                } catch {
                    this.storage.removeItem(this.storageKey);
                }
            }
        }
    }
}
