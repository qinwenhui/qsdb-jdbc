# Contributing

Thanks for considering a contribution to qsdb-jdbc!

## Development

```bash
mvn test          # unit tests — no server required
mvn package       # build the single jar
```

Integration tests run against a live QSDB server and are gated by
environment variables (`QSDB_IT_ADDR`, `QSDB_IT_TOKEN`); they are skipped
automatically when the variables are unset.

## Ground rules

- **Zero runtime dependencies** is a hard constraint: HTTP and JSON are
  built in (`internal` package). Test-scope dependencies are fine.
- Java 8 language level; JDBC 4.2 target.
- SQL is passed through verbatim — the driver never rewrites user SQL.
  Capability boundaries are adjudicated by the QSDB kernel and surfaced as
  errors, not papered over.
- Unsupported JDBC capabilities throw `SQLFeatureNotSupportedException`
  (or an explicit `SQLException`) — honest refusal over silent no-ops.
- Exception messages are in English; never include credentials.
- Run `mvn test` before submitting; keep the build green.

## Pull requests

1. Fork + branch from `main`.
2. Keep commits focused; write clear commit messages.
3. Describe what changed and why; include test coverage for behavior changes.

## License

By contributing you agree that your contributions are licensed under the
[MIT License](LICENSE).
