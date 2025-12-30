# @lumadb/supabase-py

Supabase-compatible Python SDK for LumaDB.

## Installation

```bash
pip install lumadb-supabase
```

## Usage

```python
from supabase import create_client

# Initialize client
supabase = create_client("http://localhost:3000", "your-anon-key")

# Authentication
user = await supabase.auth.sign_up(email="user@example.com", password="password123")
session = await supabase.auth.sign_in_with_password(email="user@example.com", password="password123")

# Query database
response = await supabase.from_("posts") \
    .select("id, title, author:users(name, email)") \
    .eq("published", True) \
    .order("created_at", desc=True) \
    .limit(10) \
    .execute()

posts = response.data

# Insert data
new_post = await supabase.from_("posts") \
    .insert({"title": "Hello World", "content": "My first post"}) \
    .execute()

# Update data
await supabase.from_("posts") \
    .update({"published": True}) \
    .eq("id", post_id) \
    .execute()

# Delete data
await supabase.from_("posts") \
    .delete() \
    .eq("id", post_id) \
    .execute()

# RPC calls
result = await supabase.rpc("get_user_stats", {"user_id": user_id})
```

## Synchronous API

```python
from supabase import create_client

supabase = create_client("http://localhost:3000", "your-anon-key")

# Sync operations (prefix with execute_sync)
response = supabase.from_("posts").select("*").execute_sync()
```

## API Reference

### create_client(url, key, options?)

Creates a new Supabase client instance.

### supabase.from_(table)

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
- `.is_(column, value)` - IS (null, true, false)
- `.in_(column, values)` - IN array
- `.contains(column, value)` - Contains
- `.text_search(column, query)` - Full-text search

### Ordering & Pagination

- `.order(column, desc=False)` - Order by column
- `.limit(count)` - Limit results
- `.range(start, end)` - Range of results

### supabase.auth

Authentication client with:
- `.sign_up(email, password)` - Sign up
- `.sign_in_with_password(email, password)` - Sign in
- `.sign_in_with_oauth(provider)` - OAuth sign in
- `.sign_out()` - Sign out
- `.get_session()` - Get current session
- `.get_user()` - Get current user
- `.reset_password_for_email(email)` - Password reset
- `.update_user(attributes)` - Update user
- `.refresh_session()` - Refresh token

## License

Apache-2.0
