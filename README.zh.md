# plugins

[![Go Reference](https://pkg.go.dev/badge/github.com/appootb/plugins/v2.svg)](https://pkg.go.dev/github.com/appootb/plugins/v2)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[English](README.md) | [中文](README.zh.md)

[substratum](https://github.com/appootb/substratum) 的官方插件集合，通过 blank-import 注册各能力实现，按需选用配置、发现、存储、消息队列等后端。

模块路径：`github.com/appootb/plugins/v2`  
Go 版本：`1.26+`

## 目录

| 能力 | 插件 | 说明 |
|------|------|------|
| Auth | `auth/client/rpc` | 客户端 Token 经 account 服务 RPC 解析，服务端 Token 走本地 JWT |
| Configure | `configure/backend/etcd` | etcd v3 配置中心 |
| Configure | `configure/backend/toml` | 本地 TOML 文件配置（支持热重载） |
| Credential | `credential/client/redis` | Redis 存储账号 secret seed |
| Discovery | `discovery/backend/etcd` | etcd v3 服务发现 |
| Logger | `logger/json/console` | JSON 行日志输出到 stdout/stderr |
| Queue | `queue/backend/kafka` | Kafka 消息队列 |
| Queue | `queue/backend/pulsar` | Apache Pulsar / 腾讯 TDMQ |
| Queue | `queue/idempotent/redis` | Redis 幂等消费锁 |
| Storage | `storage/sql/*` | GORM SQL 方言：MySQL / Postgres / SQLite / SQL Server / ClickHouse |
| Storage | `storage/elasticsearch/v7` | Elasticsearch 7.x |
| Task | `task/locker/redis` | Redis 分布式任务锁（SETNX + 续租） |

## 安装

```bash
go get github.com/appootb/plugins/v2
```

依赖 [substratum/v2](https://github.com/appootb/substratum)。插件在 `init()` 中向 substratum 注册实现，业务侧只需 blank-import 所需包。

## 使用方式

### 基本约定

1. **Blank-import**：在 `main` 或初始化包中 `_` 导入插件路径。
2. **环境变量**：部分后端在 `init` 时读取环境变量并立即连接；为空或连接失败会 `panic`（生产 fail-fast）。
3. **`InitComponent`**：Redis / Kafka / Pulsar 等插件默认用环境变量 `COMPONENT` 作为 storage 组件名；也可代码覆盖。
4. **互斥注册**：
   - 进程内只能注册 **一个** SQL dialect（`storage/sql/*` 任选其一）。
   - 进程内只能注册 **一个** queue backend（`kafka` 与 `pulsar` 勿同时 blank-import）。

### 示例

```go
package main

import (
	// 配置：本地 TOML，或改用 etcd
	_ "github.com/appootb/plugins/v2/configure/backend/toml"

	// 服务发现
	_ "github.com/appootb/plugins/v2/discovery/backend/etcd"

	// 日志
	_ "github.com/appootb/plugins/v2/logger/json/console"

	// 存储：SQL 方言（进程内只选一个）
	_ "github.com/appootb/plugins/v2/storage/sql/mysql"
	// 可选：Elasticsearch 7
	_ "github.com/appootb/plugins/v2/storage/elasticsearch/v7"

	// 消息队列（与 pulsar 二选一）
	_ "github.com/appootb/plugins/v2/queue/backend/kafka"
	_ "github.com/appootb/plugins/v2/queue/idempotent/redis"

	// 任务锁 / 凭证
	_ "github.com/appootb/plugins/v2/task/locker/redis"
	_ "github.com/appootb/plugins/v2/credential/client/redis"

	// 鉴权：客户端 Token 走 account RPC
	tokenrpc "github.com/appootb/plugins/v2/auth/client/rpc"
)

func main() {
	// 可选：覆盖 Redis/Kafka 等使用的 storage 组件名
	// redisidempotent.InitComponent("my-redis")

	// 鉴权 RPC 出站服务间 Token 的 key id（在对外提供服务前调用）
	tokenrpc.InitServerRPCKey(1)

	// ... 继续 substratum 应用启动
}
```

## 插件说明

### Auth — `auth/client/rpc`

- 注册 `auth.AlgorithmAuth`：客户端 Token 调用 account 的 `Token.Parse`；服务端 Token 使用 substratum 内置 JWT 解析。
- 依赖 `client.Implementor()` 与 discovery，以连接 `COMPONENT_ACCOUNT`。
- 调用 `InitServerRPCKey(serverKeyID)`，使出站 Parse RPC 带上正确的服务间签名 key。

### Configure

| 包 | 环境变量 | 地址示例 |
|----|----------|----------|
| `configure/backend/etcd` | `ETCD` | `http://user:pass@127.0.0.1:2379/config` |
| | | `http://user:pass@h1:2379,h2:2379/config`（多节点） |
| `configure/backend/toml` | `TOML` | `/etc/app/config.toml` |
| | | `file:///etc/app/config.toml#config/myapp`（`#` 后为 key 前缀） |

- **etcd**：URL path 为 key 前缀；支持 watch。
- **toml**：原子写盘（temp + rename）；外部修改经短 debounce 后热重载；支持扁平与分层 TOML。

`go test` 时上述后端会跳过自动注册，便于无集群单测。

### Discovery — `discovery/backend/etcd`

- 环境变量 `ETCD`，URL 形态与 configure/etcd 相同，例如 `http://127.0.0.1:2379/discovery`。
- 基于 etcd lease / watch 做服务注册与发现。

### Credential — `credential/client/redis`

- Redis Hash：`account:secret:seed:{uid}:hash`，field 为 keyID。
- 默认组件名：`COMPONENT`；可用 `InitComponent(name)` 覆盖。

### Logger — `logger/json/console`

- 每行一条 JSON；Error 及以上写 stderr，其余写 stdout。
- 将 `PATH` / `REQUEST` / `UID` 等字段规范化为访问日志风格输出。

### Queue

| 包 | 说明 |
|----|------|
| `queue/backend/kafka` | 同时注册 `queue.Backend` 与 Kafka storage dialect；broker 来自 storage 地址（host 可逗号分隔） |
| `queue/backend/pulsar` | Pulsar；`params.tdmq` 适配腾讯 TDMQ；`ssl=true` 切换 TLS |
| `queue/idempotent/redis` | 消费前 `SETNX` 锁（默认 TTL 2h）；失败/重入队时删 key 以便重试 |

Kafka / Pulsar 均支持 `InitComponent`。

### Storage

**SQL（GORM，进程内仅能注册一个）：**

| 包 | 备注 |
|----|------|
| `storage/sql/mysql` | 默认 `charset=utf8mb4&parseTime=True&loc=Local` |
| `storage/sql/postgres` | pgx 风格 keyword DSN |
| `storage/sql/sqlite` | 库路径取自 `Address.NameSpace` |
| `storage/sql/sqlserver` | `NameSpace` 映射为 database |
| `storage/sql/clickhouse` | clickhouse-go DSN |

**Common dialect：**

| 包 | 说明 |
|----|------|
| `storage/elasticsearch/v7` | ES 7.x；host 可逗号分隔；`ssl=true` → https |
| `queue/backend/kafka` | 同时作为 Kafka storage 方言 |
| `queue/backend/pulsar` | 同时作为 Pulsar storage 方言 |

### Task — `task/locker/redis`

- Key：`task:scheduler:locker:{scheduler}`。
- 随机 token + Lua 续租/删除，保证只释放自己的锁；`Lock` 阻塞直到获取，后台 touch 延长 TTL。

## 环境变量一览

| 变量 | 用途 |
|------|------|
| `ETCD` | configure/etcd、discovery/etcd 连接 URL |
| `TOML` | configure/toml 文件路径（可选 `#` 前缀） |
| `COMPONENT` | Redis / Kafka / Pulsar / credential / idempotent / locker 默认 storage 组件名 |

## 开发

```bash
# 运行全部单元测试
go test ./...

# 指定包
go test ./configure/backend/toml/...
```

多数后端在 `testing.Testing()` 为 true 时不自动连接外部依赖，逻辑测试无需本机 etcd/Redis。

## License

[MIT](LICENSE) © appootb
