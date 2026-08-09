# plugins

[![Go Reference](https://pkg.go.dev/badge/github.com/appootb/plugins/v2.svg)](https://pkg.go.dev/github.com/appootb/plugins/v2)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | [中文](README.zh.md)

Official plugin collection for [substratum](https://github.com/appootb/substratum). Each package registers an implementor via blank-import so you can pick configure, discovery, storage, queue, and related backends as needed.

Module path: `github.com/appootb/plugins/v2`  
Go version: `1.26+`

## Catalog

| Capability | Plugin | Description |
|------------|--------|-------------|
| Auth | `auth/client/rpc` | Client tokens via account `Token.Parse` RPC; server tokens via local JWT |
| Configure | `configure/backend/etcd` | etcd v3 config backend |
| Configure | `configure/backend/toml` | Local TOML file config (hot reload) |
| Credential | `credential/client/redis` | Account secret seeds in Redis |
| Discovery | `discovery/backend/etcd` | etcd v3 service discovery |
| Logger | `logger/json/console` | JSON line logs to stdout/stderr |
| Queue | `queue/backend/kafka` | Kafka message queue |
| Queue | `queue/backend/pulsar` | Apache Pulsar / Tencent TDMQ |
| Queue | `queue/idempotent/redis` | Redis idempotent consumer lock |
| Storage | `storage/sql/*` | GORM SQL dialects: MySQL / Postgres / SQLite / SQL Server / ClickHouse |
| Storage | `storage/elasticsearch/v7` | Elasticsearch 7.x |
| Task | `task/locker/redis` | Redis distributed task lock (SETNX + lease renew) |

## Install

```bash
go get github.com/appootb/plugins/v2
```

Depends on [substratum/v2](https://github.com/appootb/substratum). Plugins register with substratum in `init()`; application code only needs to blank-import the packages it uses.

## Usage

### Conventions

1. **Blank-import**: `_`-import the plugin path from `main` (or an init package).
2. **Environment variables**: Some backends read env vars in `init` and connect immediately; empty or failed connect `panic`s (production fail-fast).
3. **`InitComponent`**: Redis / Kafka / Pulsar-style plugins default to the `COMPONENT` env var as the storage component name; override in code when needed.
4. **Mutual exclusion**:
   - Only **one** SQL dialect may be registered per process (`storage/sql/*` — pick one).
   - Only **one** queue backend may be registered per process (do not blank-import both `kafka` and `pulsar`).

### Example

```go
package main

import (
	// Config: local TOML, or switch to etcd
	_ "github.com/appootb/plugins/v2/configure/backend/toml"

	// Service discovery
	_ "github.com/appootb/plugins/v2/discovery/backend/etcd"

	// Logging
	_ "github.com/appootb/plugins/v2/logger/json/console"

	// Storage: SQL dialect (only one per process)
	_ "github.com/appootb/plugins/v2/storage/sql/mysql"
	// Optional: Elasticsearch 7
	_ "github.com/appootb/plugins/v2/storage/elasticsearch/v7"

	// Queue (pick kafka or pulsar, not both)
	_ "github.com/appootb/plugins/v2/queue/backend/kafka"
	_ "github.com/appootb/plugins/v2/queue/idempotent/redis"

	// Task locker / credential
	_ "github.com/appootb/plugins/v2/task/locker/redis"
	_ "github.com/appootb/plugins/v2/credential/client/redis"

	// Auth: client tokens via account RPC
	tokenrpc "github.com/appootb/plugins/v2/auth/client/rpc"
)

func main() {
	// Optional: override storage component used by Redis/Kafka plugins
	// redisidempotent.InitComponent("my-redis")

	// Key id for outbound service-to-service token on auth Parse RPCs
	// (call before serving)
	tokenrpc.InitServerRPCKey(1)

	// ... continue substratum app startup
}
```

## Plugin details

### Auth — `auth/client/rpc`

- Registers `auth.AlgorithmAuth`: client tokens call account `Token.Parse`; server tokens use substratum built-in JWT parse.
- Requires `client.Implementor()` and discovery so the process can reach `COMPONENT_ACCOUNT`.
- Call `InitServerRPCKey(serverKeyID)` so outbound Parse RPCs attach the correct server signing key.

### Configure

| Package | Env | Address examples |
|---------|-----|------------------|
| `configure/backend/etcd` | `ETCD` | `http://user:pass@127.0.0.1:2379/config` |
| | | `http://user:pass@h1:2379,h2:2379/config` (multi-host) |
| `configure/backend/toml` | `TOML` | `/etc/app/config.toml` |
| | | `file:///etc/app/config.toml#config/myapp` (fragment = key prefix) |

- **etcd**: URL path is the key prefix; supports watch.
- **toml**: atomic writes (temp + rename); external edits reload after a short debounce; supports flat and hierarchical TOML.

During `go test`, these backends skip auto-registration so unit tests can run without a live cluster.

### Discovery — `discovery/backend/etcd`

- Env `ETCD`, same URL shape as configure/etcd, e.g. `http://127.0.0.1:2379/discovery`.
- Service register/discover via etcd lease and watch.

### Credential — `credential/client/redis`

- Redis hash: `account:secret:seed:{uid}:hash`, field = keyID.
- Default component: `COMPONENT`; override with `InitComponent(name)`.

### Logger — `logger/json/console`

- One JSON object per line; Error and above go to stderr, lower levels to stdout.
- Normalizes fields such as `PATH` / `REQUEST` / `UID` for access-log style output.

### Queue

| Package | Description |
|---------|-------------|
| `queue/backend/kafka` | Registers both `queue.Backend` and a Kafka storage dialect; brokers from storage address (comma-separated hosts) |
| `queue/backend/pulsar` | Pulsar; `params.tdmq` for Tencent TDMQ; `ssl=true` enables TLS |
| `queue/idempotent/redis` | `SETNX` lock before process (default TTL 2h); deletes key on Failed/Requeued so retries can proceed |

Kafka and Pulsar both support `InitComponent`.

### Storage

**SQL (GORM; only one dialect per process):**

| Package | Notes |
|---------|-------|
| `storage/sql/mysql` | Defaults: `charset=utf8mb4&parseTime=True&loc=Local` |
| `storage/sql/postgres` | pgx-style keyword DSN |
| `storage/sql/sqlite` | DB path from `Address.NameSpace` |
| `storage/sql/sqlserver` | `NameSpace` mapped to database |
| `storage/sql/clickhouse` | clickhouse-go DSN |

**Common dialects:**

| Package | Description |
|---------|-------------|
| `storage/elasticsearch/v7` | ES 7.x; comma-separated hosts; `ssl=true` → https |
| `queue/backend/kafka` | Also a Kafka storage dialect |
| `queue/backend/pulsar` | Also a Pulsar storage dialect |

### Task — `task/locker/redis`

- Key: `task:scheduler:locker:{scheduler}`.
- Random token + Lua renew/delete so only the owner releases the lock; `Lock` blocks until acquired; background touch extends TTL.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `ETCD` | Connection URL for configure/etcd and discovery/etcd |
| `TOML` | Path for configure/toml (optional `#` key prefix) |
| `COMPONENT` | Default storage component for Redis / Kafka / Pulsar / credential / idempotent / locker |

## Development

```bash
# All unit tests
go test ./...

# One package
go test ./configure/backend/toml/...
```

Most backends skip connecting to external deps when `testing.Testing()` is true, so pure logic tests do not need local etcd/Redis.

## License

[MIT](LICENSE) © appootb
