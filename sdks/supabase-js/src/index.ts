/**
 * LumaDB Supabase-Compatible TypeScript SDK
 *
 * Drop-in replacement for @supabase/supabase-js that connects to LumaDB
 *
 * @example
 * ```typescript
 * import { createClient } from '@lumadb/supabase-js';
 *
 * const supabase = createClient('http://localhost:3000', 'your-anon-key');
 *
 * // Auth
 * const { data: user, error } = await supabase.auth.signUp({
 *   email: 'user@example.com',
 *   password: 'password123'
 * });
 *
 * // Database
 * const { data, error } = await supabase
 *   .from('posts')
 *   .select('*, author:users(*)')
 *   .eq('published', true)
 *   .order('created_at', { ascending: false })
 *   .limit(10);
 * ```
 */

export { createClient, SupabaseClient } from './client';
export { SupabaseAuthClient } from './auth';
export { PostgrestClient, PostgrestQueryBuilder, PostgrestFilterBuilder } from './postgrest';
export type {
    SupabaseClientOptions,
    AuthResponse,
    AuthError,
    User,
    Session,
    PostgrestResponse,
    PostgrestSingleResponse,
    PostgrestError,
} from './types';
