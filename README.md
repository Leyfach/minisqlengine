# minisqlengine

Experimental mini SQL engine with a Rust core library and a Go API layer.
The REST service exposes a simple SQL execution endpoint with a stable
JSON contract and optional auth/audit hooks.

## Structure

- `core/` – Rust library with table storage, basic engine, and a simple parser built with `nom`.
- `server/` – Go HTTP server exposing the core via a `/query` endpoint and
providing optional authorization and audit logging. A stub `gRPC` entry
point is reserved for future binary/streaming access.
- `docker-compose.yml` – Compose file wiring Rust core and Go server containers.
- `Makefile` – helper targets for building and testing both components.

## Testing

```sh
make test
```

## Example SQL

```
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users WHERE id=1;
```

## HTTP API

`POST /query` accepts a JSON body:

```json
{
  "sql": "SELECT * FROM users",
  "limit": 10,          // optional pagination limit
  "offset": 0,          // optional pagination offset
  "timeout_ms": 1000    // optional execution timeout
}
```

The response always follows:

```json
{
  "columns": ["id", "name"],
  "rows": [[1, "Alice"]],
  "error": {"code": 123, "message": "details"} // present only on error
}
```

HTTP codes reflect success or the encountered error (e.g. `400` for bad
requests, `401` for unauthorized, `408` for timeouts).

Authorization is controlled via the `API_TOKEN` environment variable. If
set, clients must send `Authorization: Bearer <token>`; this check can be
disabled in development by setting `DEV_MODE=1`. All queries are logged
for audit purposes.

## Rust ↔ Go Integration

The long‑term boundary between the Rust core and Go frontends is a small
FFI layer compiled as a `cdylib`. Go calls exported C functions via
`cgo`, receiving JSON blobs and error codes which map to the schema
above. This isolates unsafe calls and keeps error handling uniform. A
separate RPC process is an alternative for horizontal scaling and remote
execution.

## gRPC (experimental)

A gRPC service mirroring `/query` is planned for efficient binary
transport and streaming results. The `server` module contains a placeholder for
future implementation; contributors can add a `Query` RPC with the same
request/response messages as the HTTP API.
