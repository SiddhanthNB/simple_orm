# Simple ORM - TODO List

## Database Support
- [ ] **Multi-database driver support**: Enhance `configure_database()` to support MySQL, SQLite, Oracle, SQL Server with appropriate sync/async drivers (PostgreSQL already works perfectly with psycopg3)

## Query Builder
- [ ] **QueryBuilder pattern**: Create fluent query interface with method chaining (`.where()`, `.order_by()`, `.limit()`, `.join()`) that produces a `Run` object supporting both `.run()` (sync) and `.run.async_(session)` (async) execution