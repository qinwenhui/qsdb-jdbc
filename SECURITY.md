# Security Policy

## Reporting a vulnerability

Please do **not** open a public issue for security reports.

Open a [GitHub security advisory](https://github.com/qinwenhui/qsdb-jdbc/security/advisories/new)
or contact the maintainer directly. You will get a response within a few days.

## Scope

- This driver speaks plain HTTP to a QSDB server. Transport encryption is
  expected to be terminated by a reverse proxy in front of the server; the
  driver rejects `ssl=true` rather than pretending to encrypt.
- Credentials (static token, or user/password for the login session) are
  sent only via the `Authorization` header, never in URLs, logs, or
  `toString()` output.

## Supported versions

Only the latest released version is supported with security fixes.
