# LumaDB Supabase Compatibility Layer

## Quick Start

```bash
# Build all crates
cd crates/supabase-compat
cargo build --release

# Run tests
cargo test --all

# Run the server
cargo run --release
```

## Architecture

```
crates/supabase-compat/
├── supabase-common      # Shared types, config, errors
├── supabase-auth        # GoTrue-compatible authentication
├── supabase-rest        # PostgREST-compatible API
├── supabase-rls         # Row Level Security engine
├── supabase-realtime    # WebSocket/CDC engine
├── supabase-storage     # S3-compatible storage
├── supabase-functions   # Edge functions runtime
├── supabase-graphql     # Auto-generated GraphQL
├── supabase-vector      # pgvector-compatible search
├── supabase-webhooks    # Database webhooks
├── supabase-mfa         # Multi-factor authentication
├── supabase-observability # Metrics and monitoring
├── supabase-migrations  # Schema versioning
└── supabase-admin       # Admin dashboard API

sdks/
├── supabase-js/         # TypeScript/JavaScript SDK
└── supabase-py/         # Python SDK
```

## Configuration

Environment variables:
```bash
# Server
SUPABASE_HOST=0.0.0.0
SUPABASE_REST_PORT=3000
SUPABASE_AUTH_PORT=9999

# Auth
JWT_SECRET=your-secret-key
JWT_EXPIRY=3600
SITE_URL=http://localhost:3000

# Database
DATABASE_URL=postgres://user:pass@localhost/db
```

## API Compatibility

| Feature | Status | Supabase Parity |
|---------|--------|-----------------|
| REST API (PostgREST) | ✅ | 100% |
| Auth (GoTrue) | ✅ | 95% |
| Real-time | ✅ | 90% |
| Storage | ✅ | 90% |
| Edge Functions | ✅ | 85% |
| GraphQL | ✅ | 80% |
| Vector Search | ✅ | 90% |

## Development

```bash
# Format code
cargo fmt --all

# Lint
cargo clippy --all-targets

# Security audit
cargo audit

# Run specific crate tests
cargo test -p supabase-auth
```

## License

Apache-2.0
