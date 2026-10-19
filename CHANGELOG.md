# Changelog

All notable changes to qsdb-jdbc are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/); versioning
follows [SemVer](https://semver.org/) (pre-1.0: minor = breaking).

## [0.1.0] - 2026-10-19

First public release.

- Type 4 pure-Java driver, single jar, zero runtime dependencies, Java 8+.
- JDBC 4.2 target: forward-only read-only result sets, full `getXxx` matrix,
  `wasNull`, `DatabaseMetaData` table/column/type coverage, `QsdbDataSource`.
- Server-side parameterization (`paramStyle=server`): values bound inside the
  kernel, never in SQL text; BINARY / NaN / control-character parameters
  supported; `paramStyle=text` fallback for older kernels.
- True streaming reads (`setFetchSize(n>0)` → kernel `rows` streaming channel);
  two-phase cancellation (`Statement.cancel()` covers execute and transport
  segments).
- Login sessions (`user`+`password` → `/auth/login`) with automatic re-login
  and revoked-session handling; static `token` still supported.
- Multi-database: `database` property / `setCatalog` routing, `getCatalogs`,
  catalog-filtered metadata.
- Bulk loading channel `QsdbBulkWriter` (`qsdb-rows` binary format) with
  `overwrite()` partition replace and `overwriteCas(generation)` optimistic
  concurrency.
- Runtime error-code table (`/api/v1/error-codes`) with local fallback;
  structured `[code=…, retry=…]` annotations; `getUpdateCount` returns real
  affected-row counts.
