# GTD

一个按照 [GTD in 15 minutes](https://hamberg.no/gtd) 工作流实现的任务管理系统。项目由同一个 `gtd` 二进制提供 HTTP Server、普通 CLI 和基于 Ratatui 的交互式终端界面，数据保存在 SQLite 中。

## 核心模型

- 每个 task 最初只需要一段描述，并进入 `in` 列表。
- 一个 task 始终属于且只属于一个列表：`in`、`next-action`、`waiting-for`、`someday-maybe` 或 `archive`。
- 状态为 `pending`、`doing`、`done` 或 `trash`。
- context 由可重复的 `key:value` labels 和一段可选 note 组成。
- project 不单独建表，使用例如 `project:gtd` 的 label 表示。

数据库约束保证：`archive` 中只能存在 `done/trash`，其他列表中只能存在 `pending/doing`。

## 快速开始

需要 Rust 1.98+ 和 `musl-gcc`（Debian/Ubuntu 可安装 `musl-tools`）。SQLite 已使用 bundled feature，不需要预装 SQLite 服务或开发包。

启动 Server（首次启动会自动运行 Diesel migration）：

```bash
cargo run -- server --database ./gtd.db
```

另开一个终端使用 CLI：

```bash
cargo run -- add 写一份 GTD 项目说明
cargo run -- process
cargo run -- list next-action --label project:gtd
cargo run -- pick                 # TTY 中交互选择
cargo run -- pick 1               # 也可以直接指定 ID
cargo run -- done 1 --label result:published --note "reviewed"
cargo run -- review
```

Server 默认为 `http://127.0.0.1:4040`。远程 Server 可通过全局参数或环境变量指定：

```bash
gtd --server-url http://server.example:4040 list in
GTD_SERVER_URL=http://server.example:4040 gtd add "capture this"
```

## Make 与 Docker

项目只有一个 `gtd` binary。Makefile 直接提供 `build`、`run`、`image` 和 `deploy`，运行模式通过 `ARGS` 传给同一个 binary。运行 `make` 或 `make help` 可以查看完整命令。

本地构建与运行：

```bash
make build
make run
make run ARGS='add 写一份部署说明'
```

`make build` 和 `make run` 默认使用 `x86_64-unknown-linux-musl`，因此本地产物也是静态链接 binary；所需 Rust target 由 `rust-toolchain.toml` 自动安装。不传 `ARGS` 时，`make run` 默认启动本地 Server。需要 release 构建时可运行 `make build BUILD_FLAGS=--release`。

构建 Docker 镜像：

```bash
make image
make image IMAGE=registry.example/gtd:v1
```

直接使用 Docker CLI 后台部署：

```bash
make deploy
make run ARGS='add 容器中的任务'
make run ARGS='list in'
```

`make run ARGS='…'` 在本机运行 CLI，默认访问 `http://127.0.0.1:4040`。再次运行 `make deploy` 会重建镜像并替换同名容器，但不会删除保存 SQLite 数据的 volume。

默认镜像为 `gtd:local`，Server 端口为 `4040`。可以覆盖 Make 变量，例如：

```bash
make deploy IMAGE=registry.example/gtd:v1 PORT=8080 VOLUME=gtd-production
```

`rust-toolchain.toml` 是 Rust 版本的唯一来源。Makefile 从其中读取 `channel` 并作为 build argument 传给 Docker；Dockerfile 会再次校验版本，传入不一致的版本会立即失败。

Docker builder 在复制 Cargo manifests 后先单独执行依赖下载，因此修改业务代码不会使依赖下载缓存失效。builder 使用 Alpine/musl 为当前镜像架构生成静态链接 binary，并在最终 `scratch` 阶段运行 `/gtd --help`；binary 缺失、架构不匹配或依赖动态加载器都会直接导致镜像构建失败。runtime 只包含 `/gtd` 和可写的 `/data`。容器以 UID/GID `10001:10001` 运行，并将 SQLite 数据持久化到 `/data`。scratch 中没有 shell 或 curl；应用仍提供 `/health` HTTP 端点，可由 Docker 外部或编排器探测。

## 命令

### `add`

只接受描述。Server 原子地创建 `in/pending` task，不允许在捕捉阶段附加分类信息。

### `pick`

从 `next-action/pending` 中选择 task，并原子地改为 `doing`。省略 ID 时进入 Ratatui 选择器；在管道等非 TTY 环境中必须显式提供 ID。

### `done`

只接受 `doing` task，将其改为 `done` 并移动到 `archive`。完成时可用多个 `--label key:value` 和 `--note` 补充 context。

### `list`

列出指定列表，并可用多个 label 做 AND 过滤：

```bash
gtd list next-action --label project:gtd --label place:home
gtd list archive --state done --json
```

### `process`

使用 Ratatui 依次处理 `in/pending` task：

- actionable：`do it now`、`defer` 或 `delegate`；
- non-actionable：`trash` 或 `maybe`；
- defer/delegate/done 时可录入 labels 与 note；
- maybe 必须指定 `30m`、`2h`、`7d`、`4w` 等回访时长；
- do it now 会先保存为 `doing`，再等待 done/trash 结果，因此意外退出也不会丢失当前状态。

### `review`

使用 Ratatui 依次检查 `next-action` 和 `someday-maybe`：

- next-action 可以保留、移动到 someday/maybe 或 trash；
- someday/maybe 可以保留、激活到 next-action 或 trash；
- 从 review 放入 someday/maybe 时，可设置回访时长，也可永久保留为 someday。

所有交互决定都会立即调用一个原子 Server API。HTTP 客户端复用 keep-alive 连接，因此长时间运行的 process/review 不依赖单个易中断的大事务。

## 自动回访

带 `revisit_at` 的 someday/maybe task 到期后会自动回到 `in/pending`。Server 每 30 秒检查一次；任何列表查询也会先触发一次检查。

## HTTP API

| Method | Path | 作用 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/api/tasks` | 创建 `in/pending` task |
| `GET` | `/api/tasks?list=...&state=...&labels=k:v,k2:v2` | 查询与过滤 |
| `GET` | `/api/tasks/{id}` | 查询单个 task |
| `POST` | `/api/tasks/{id}/actions/{action}` | 执行原子状态转换 |
| `GET` | `/api/events` | Server-Sent Events 长连接 |

支持的 action 为 `start`、`pick`、`done`、`trash`、`defer`、`delegate`、`maybe` 和 `activate`。转换请求体示例：

```json
{
  "context": {
    "labels": { "project": "gtd", "place": "home" },
    "note": "需要先看草稿"
  },
  "revisit_at": null
}
```

监听实时变更（事件类型为 `created`、`updated` 或 `revived`）：

```bash
curl -N http://127.0.0.1:4040/api/events
```

Server 默认只绑定 loopback，当前版本没有认证。不要在没有反向代理和认证层的情况下直接暴露到公网。

## 架构

```text
CLI / Ratatui TUI
        │ HTTP + SSE
        ▼
    Axum Server
        │ TaskRepository trait
        ▼
Diesel SQLite repository
```

Axum 只依赖 `TaskRepository` trait，不直接依赖 Diesel 类型。`SqliteRepository` 是当前实现，负责连接池、migration、事务、label 合并和到期任务回收；以后可以增加 PostgreSQL 等实现而不改 CLI 或 HTTP handler。

## 开发验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
