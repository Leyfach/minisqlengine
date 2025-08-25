# minisqlengine

Experimental mini SQL engine with a Rust core library and a Go REST API.

## Structure

- `core/` – Rust library with table storage, basic engine, and a simple parser built with `nom`.
- `server/` – Go HTTP server using `chi` to expose the core via a `/query` endpoint.
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
