# Simple ORM - TODO List

## Database Support
- [ ] **Multi-database driver support**: Enhance `configure_database()` to support MySQL, SQLite, Oracle, SQL Server with appropriate sync/async drivers (PostgreSQL already works perfectly with psycopg3)

## Query Builder
- [ ] **QueryBuilder pattern**: Create fluent query interface with method chaining (`.where()`, `.order_by()`, `.limit()`, `.join()`) that produces a `Run` object supporting both `.run()` (sync) and `.run.async_(session)` (async) execution
- [ ] **Raw SQL where clauses**: Support raw SQL in where() method: `User.query.where("age > ? AND (status = ? OR priority = ?)", [18, "active", "high"])`
- [ ] **Additional operators**: Add more Django-style operators like `range`, `year`, `month`, `date`, `icontains`, `regex`, etc.
- [ ] **Raw query method**: Add `User.query.raw()` method for complete raw SQL queries
