# @lumadb/supabase-js

Supabase-compatible TypeScript SDK for LumaDB.

## Installation

```bash
npm install @lumadb/supabase-js
```

## Usage

```typescript
import { createClient } from '@lumadb/supabase-js';

// Initialize client
const supabase = createClient('http://localhost:3000', 'your-anon-key');

// Authentication
const { data: user, error } = await supabase.auth.signUp({
  email: 'user@example.com',
  password: 'password123'
});

const { data: session } = await supabase.auth.signInWithPassword({
  email: 'user@example.com',
  password: 'password123'
});

// Query database
const { data: posts, error } = await supabase
  .from('posts')
  .select('id, title, author:users(name, email)')
  .eq('published', true)
  .order('created_at', { ascending: false })
  .limit(10);

// Insert data
const { data: newPost, error } = await supabase
  .from('posts')
  .insert({ title: 'Hello World', content: 'My first post' })
  .select()
  .single();

// Update data
const { error } = await supabase
  .from('posts')
  .update({ published: true })
  .eq('id', postId);

// Delete data
const { error } = await supabase
  .from('posts')
  .delete()
  .eq('id', postId);

// RPC calls
const { data, error } = await supabase.rpc('get_user_stats', {
  user_id: userId
});
```

## API Reference

### createClient(url, key, options?)

Creates a new Supabase client instance.

### supabase.from(table)

Start a query on a table. Returns a query builder with:
- `.select(columns)` - Select columns
- `.insert(values)` - Insert rows
- `.update(values)` - Update rows
- `.delete()` - Delete rows
- `.upsert(values)` - Upsert rows

### Query Filters

- `.eq(column, value)` - Equal
- `.neq(column, value)` - Not equal
- `.gt(column, value)` - Greater than
- `.gte(column, value)` - Greater than or equal
- `.lt(column, value)` - Less than
- `.lte(column, value)` - Less than or equal
- `.like(column, pattern)` - LIKE pattern
- `.ilike(column, pattern)` - Case-insensitive LIKE
- `.is(column, value)` - IS (null, true, false)
- `.in(column, values)` - IN array
- `.contains(column, value)` - Contains
- `.containedBy(column, value)` - Contained by
- `.textSearch(column, query)` - Full-text search

### Ordering & Pagination

- `.order(column, { ascending: true })` - Order by column
- `.limit(count)` - Limit results
- `.range(from, to)` - Range of results

### supabase.auth

Authentication client with:
- `.signUp({ email, password })` - Sign up
- `.signInWithPassword({ email, password })` - Sign in
- `.signInWithOAuth({ provider })` - OAuth sign in
- `.signInWithOtp({ email })` - Magic link
- `.signOut()` - Sign out
- `.getSession()` - Get current session
- `.getUser()` - Get current user
- `.resetPasswordForEmail(email)` - Password reset
- `.updateUser(attributes)` - Update user
- `.refreshSession()` - Refresh token

## License

Apache-2.0
