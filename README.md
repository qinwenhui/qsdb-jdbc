# qsdb-jdbc

**The official JDBC driver for [QSDB](https://github.com/qinwenhui/qsdb).
Type 4, pure Java, zero dependencies. Java 8 ready.**

qsdb-jdbc is the official JDBC driver for QSDB, the high-performance
analytical database: a Type 4 pure-Java implementation in a single jar with
no third-party dependencies (HTTP and JSON are built in). Java 8 and up,
targeting the JDBC 4.2 specification — connect to QSDB exactly the way you
connect to MySQL.

```java
Connection conn = DriverManager.getConnection(
    "jdbc:qsdb://qsdb-host:9430?token=xxx");
try (PreparedStatement ps = conn.prepareStatement(
        "SELECT DT, COUNT(*) AS n FROM orders WHERE DT > ? GROUP BY DT")) {
    ps.setDate(1, Date.valueOf("2026-10-01"));
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getString(1) + " " + rs.getLong(2));
        }
    }
}
```

Works out of the box with MyBatis / Spring JdbcTemplate / Druid / HikariCP /
DBeaver / DataGrip / FineReport.

## The QSDB ecosystem

| Project | Description |
|---|---|
| [qsdb](https://github.com/qinwenhui/qsdb) | The QSDB database kernel and HTTP server |
| [qsdb-jdbc](https://github.com/qinwenhui/qsdb-jdbc) | This JDBC driver |
| [qsdb-odbc](https://github.com/qinwenhui/qsdb-odbc) | ODBC driver |
| [qsdb-sync](https://github.com/qinwenhui/qsdb-sync) | Data sync / replication between instances |
| [qsdb-studio](https://github.com/qinwenhui/qsdb-studio) | Visual client (GUI workbench) |

## Connecting

```text
jdbc:qsdb://host:port[?prop=value&prop=value]
```

| Property | Type | Default | Description |
|---|---|---|---|
| `token` | String | — | Static bearer token (server `--token`) |
| `user` + `password` | String | — | **Login session**: Basic auth exchanged for a process-lifetime token via `POST /api/v1/auth/login`; revoked sessions re-login automatically (reads are safely retried once, writes only re-login). `password` requires `user` |
| `database` | String | — | Default database on multi-mount servers; equivalent to `Connection.setCatalog` |
| `connectTimeoutMs` | int | 5000 | TCP connect timeout |
| `queryTimeoutMs` | int | 0 (unlimited) | Default query timeout; `Statement.setQueryTimeout` overrides per statement |
| `maxRows` | int | 0 (unlimited) | Default row limit per statement (`Statement.setMaxRows` overrides) |
| `readFormat` | String | `auto` | Query response format: `auto` (prefer the binary `rows` channel, fall back to `json` on old servers) \| `json` \| `rows` |
| `paramStyle` | String | `server` | Prepared-statement parameter channel: `server` (values travel in the request body and are bound **inside the kernel** — never in SQL text) \| `text` (client-side literal rendering, for pre-2026-10 kernels) |
| `batchRewrite` | boolean | `true` | Collapse consecutive `executeBatch` INSERTs into multi-row VALUES (SQL channel) |
| `transport` | String | `http` | Only `http` in this release |
| `ssl` | boolean | `false` | Reserved; must be `false` (terminate TLS with a reverse proxy) |

Spring wiring: `cn.qinwh.qsdb.jdbc.QsdbDataSource` (`setUrl` / `setToken`).

## Features

- **Standard JDBC shell**: forward-only read-only result sets, full `getXxx`
  type coverage, `wasNull` semantics, **server-side parameterization** —
  `?` values travel in the request body and are bound inside the kernel's
  binder, so they never enter SQL text (injection safety is the kernel's
  guarantee, not a driver-side escaping scheme). BINARY / NaN / control
  characters are all legal parameters.
- **True streaming for large results**: `Statement.setFetchSize(n > 0)`
  switches to the kernel's streaming `rows` channel — chunked decoding with
  constant memory; `setMaxRows` truncation closes the connection mid-stream
  (no wasted transfer); `Statement.cancel()` supports the transport segment
  (two-phase stop semantics).
- **SQL capability aligned with the engine**: INSERT (VALUES), partition-
  granular DELETE, DDL passthrough; `executeUpdate` returns affected rows;
  UPDATE/MERGE/REPLACE/UPSERT are explicitly rejected (the storage model has
  no row-level update).
- **Batch loading channel**: `QsdbBulkWriter` speaks the kernel's zero-
  dependency `qsdb-rows` binary format (`/write?format=rows`) — ~1M rows in
  0.75s (about 1.3M rows/s, roughly 13× the SQL channel), streaming with
  constant memory. `overwrite()` replaces a whole partition; `overwriteCas(g)`
  adds optimistic concurrency (`WRITE_CONFLICT` on stale generation).
- **Multi-database**: `?database=app` or `setCatalog("app")` routes queries
  to a mounted database; `getCatalogs()` lists mounts; metadata methods
  filter by catalog.
- **Complete metadata**: DatabaseMetaData table/column/type methods +
  `getTypeInfo` (DBeaver/FineReport browsing works).
- **Pluggable transport**: HTTP now (zero server configuration); the
  transport interface is prepared for Arrow Flight.
- **Zero-dependency single jar**: drop it into any microservice, no
  transitive dependencies.

## Bulk loading

For million-row loads use the bulk channel, not `executeBatch`:

```java
try (QsdbBulkWriter w = QsdbBulkWriter.open(conn, "orders")) {
    for (Order o : orders) {
        w.setLong("id", o.id);
        w.setString("name", o.name);
        w.setDate("dt", o.dt);
        w.addRow();        // encodes + submits a chunk every 65536 rows (constant memory)
    }
}   // close() submits the remainder
```

- Column order/types come from the table schema (partition columns included);
  the value API mirrors `PreparedStatement`.
- Concurrent loading: one writer per connection (4–8 concurrent connections
  is the sweet spot).

## Build and test

```bash
mvn test                    # unit tests (no server needed)
QSDB_IT_ADDR=127.0.0.1:9430 \  # integration tests (gated; skipped when unset)
QSDB_IT_TOKEN=xxx mvn test
mvn package                 # single jar
```

A Java 8 single-file example: [`examples/Demo.java`](examples/Demo.java).

## Capability boundary (honest v1 statement)

- No transactions: `autoCommit` is always true; each write takes effect
  immediately and independently; UPDATE/MERGE/REPLACE/UPSERT are explicitly
  rejected (`0A000`).
- **SQL capability is adjudicated by the kernel** (the driver passes SQL
  through verbatim and never rewrites it). The authoritative SQL capability
  list lives in the [qsdb](https://github.com/qinwenhui/qsdb) repository
  documentation. Partitioned tables require at least one non-partition column.
- Pagination works (`LIMIT n OFFSET m`, `LIMIT m, n`, `OFFSET m`,
  `FETCH FIRST n ROWS ONLY`, `ORDER BY … NULLS FIRST|LAST`) — BI tools can
  page directly. `setMaxRows` truncates on the driver side; real paging uses
  SQL `LIMIT/OFFSET`.
- Not supported: `getGeneratedKeys`, Savepoint, scrollable cursors, LOB
  locators, SSL, Flight transport.
- `Statement.cancel()`: precise cancellation (queries pre-register a client
  id via the `X-QSDB-Query-Id` header).
- Error codes: the SQLState mapping is fetched at runtime
  (`/api/v1/error-codes`) with a local fallback; exception messages carry
  `[code=…, retry=…]` annotations (e.g. `TOO_MANY_QUERIES` is worth a
  backoff-retry while `READ_ONLY` requires switching instances).
- Pair with a kernel built after 2026-10; older servers fall back to
  JSON-only reads and client-side literal rendering.

## License

[MIT](LICENSE)
