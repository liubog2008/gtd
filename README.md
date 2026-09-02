# GTD

A task management system based on the [GTD in 15 minutes](https://hamberg.no/gtd)
workflow. A single `gtd` binary provides the HTTP server, a regular CLI, and a
Ratatui-based interactive terminal interface. Data is stored in SQLite.

## Core model

- A task initially needs only a description and enters the `in` list.
- A task always belongs to exactly one list: `in`, `next-action`, `waiting-for`,
  `someday-maybe`, or `archive`.
- A task state is `pending`, `doing`, `done`, or `trash`.
- Labels are top-level task fields expressed as repeatable `key:value` pairs.
- Projects do not have a separate table; use a label such as `project:gtd`.

Task IDs are UUID v7 values. The database stores every change in the append-only
`task_events` table, including the complete state after the change, the mutable
fields before the change, and `prev_updated_at`. Because `created_at` is invariant
for the lifetime of a task, each event stores one `created_at` and does not define
`prev_created_at`. Current state is derived from each task's latest event with a
loose scan over `(task_id, revision DESC)`; there is no current-state projection.
Database constraints allow only `done/trash` in `archive` and only `pending/doing`
in every other list.

List queries use `(list_name, task_id, revision DESC)` to narrow candidates and
then verify the list on the latest event. State and label filters currently run
in the application layer against latest state.

The current release uses the event-sourced schema directly. It is incompatible
with the old `tasks + labels` database and does not provide an automatic data
migration. Use a new database when upgrading.

## Quick start

Rust 1.98+ and `musl-gcc` are required (`musl-tools` on Debian or Ubuntu). SQLite
uses the bundled feature, so no system SQLite service or development package is
needed.

Start the server. Diesel migrations run automatically on first startup:

```bash
cargo run -- server --database ./gtd.db
```

Use the CLI from another terminal:

```bash
cargo run -- add "Write the GTD project guide"
cargo run -- process
cargo run -- list next-action --label project:gtd
cargo run -- pick                  # Select interactively in a TTY
cargo run -- pick 019d...          # Or specify a UUID directly
cargo run -- done 019d... --label result:published --description "Write and publish the guide"
cargo run -- review
```

The server defaults to `http://127.0.0.1:4040`. Select a remote server with the
global option or environment variable:

```bash
gtd --server-url http://server.example:4040 list in
GTD_SERVER_URL=http://server.example:4040 gtd add "capture this"
```

## Make and Docker

The project has one `gtd` binary. The Makefile provides `build`, `unit`, `lint`,
`run`, `image`, and `deploy`; pass the execution mode through `ARGS`. Run `make`
or `make help` to see all commands.

Build and run locally:

```bash
make build
make run
make run ARGS='add "Write the deployment guide"'
```

`make build` and `make run` target `x86_64-unknown-linux-musl` by default, so the
local artifact is statically linked. `rust-toolchain.toml` installs the required
Rust target automatically. Without `ARGS`, `make run` starts the local server.
Use `make build BUILD_FLAGS=--release` for a release build.

Build a Docker image:

```bash
make image
make image IMAGE=registry.example/gtd:v1
```

Deploy in the background with the Docker CLI:

```bash
make deploy
make run ARGS='add "A task in the container"'
make run ARGS='list in'
```

`make run ARGS='...'` runs the CLI on the host and connects to
`http://127.0.0.1:4040` by default. Running `make deploy` again rebuilds the image
and replaces the container without deleting the volume that stores SQLite data.

The default image is `gtd:local`, and the default server port is `4040`. Make
variables can be overridden:

```bash
make deploy IMAGE=registry.example/gtd:v1 PORT=8080 VOLUME=gtd-production
```

`rust-toolchain.toml` is the single source of truth for the Rust version. The
Makefile reads its `channel` and passes it to Docker as a build argument. The
Dockerfile verifies the version again and fails immediately if they differ.

The Docker builder downloads dependencies after copying only the Cargo manifests,
so application-code changes preserve the dependency cache. It builds a statically
linked binary for the current image architecture with Alpine/musl and runs
`/gtd --help` in the final `scratch` stage. A missing binary, wrong architecture,
or dynamic-loader dependency therefore fails the image build. The runtime contains
only `/gtd` and writable `/data`. It runs as UID/GID `10001:10001` and persists
SQLite data under `/data`. The scratch image has no shell or curl, but `/health`
remains available to external Docker or orchestrator health checks.

## Commands

### `add`

Accepts only a description. The server atomically creates an `in/pending` task;
classification data cannot be attached during capture.

### `pick`

Selects a `next-action/pending` task and atomically changes it to `doing`. Omitting
the ID opens the Ratatui selector. Non-TTY environments such as pipelines must
provide an ID explicitly.

### `done`

Accepts only a `doing` task, changes it to `done`, and moves it to `archive`.
Repeat `--label key:value` to add classifications, or use `--description` to
replace the description with additional information.

### `list`

Lists one GTD list and supports AND filtering across multiple labels:

```bash
gtd list next-action --label project:gtd --label place:home
gtd list archive --state done --json
```

### `process`

Uses Ratatui to process `in/pending` tasks in sequence:

- actionable tasks: `do it now`, `defer`, or `delegate`;
- non-actionable tasks: `trash` or `maybe`;
- defer, delegate, and done can add labels and edit the description;
- maybe moves the task to someday/maybe for later manual activation during review;
- do it now first persists `doing` and then waits for done/trash, so an unexpected
  exit does not lose the current state.

### `review`

Uses Ratatui to review `next-action` and `someday-maybe` in sequence:

- next-action can remain unchanged or move to someday/maybe or trash;
- someday/maybe can remain unchanged or activate into next-action or trash;
- a task moved to someday/maybe during review can be activated later.

Every interactive decision immediately invokes one atomic server API. The HTTP
client reuses keep-alive connections, so long process/review sessions do not rely
on a single fragile transaction.

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Health check |
| `POST` | `/api/v1/tasks` | Create an `in/pending` task |
| `GET` | `/api/v1/tasks?list=...&state=...&labels=k:v,k2:v2` | List and filter tasks |
| `GET` | `/api/v1/tasks/{id}` | Get one task |
| `PUT` | `/api/v1/tasks/{id}` | Conditionally replace the mutable task state |
| `GET` | `/api/v1/tasks?watch=true&revision=...&list=...&state=...&labels=...` | Open and optionally filter a Server-Sent Events stream |

Task API responses use the following shape. `metadata.revision` is the event
revision that produced the current task state and the optimistic-concurrency token
for the next PUT. It is not a task version.

```json
{
  "metadata": {
    "id": "019c6f7e-7f21-7a37-8cb6-91a4bc5fbef1",
    "revision": 41,
    "created_at": "2026-08-29T10:00:00Z",
    "updated_at": "2026-08-29T10:00:00Z"
  },
  "description": "Write the guide",
  "list": "in",
  "state": "pending",
  "labels": {}
}
```

Regular list responses use a Kubernetes-style `metadata + items` envelope. The
list metadata revision is the global read watermark for the snapshot. Each item's
`metadata.revision` belongs to that task, so the values can differ:

```json
{
  "metadata": { "revision": 73 },
  "items": [
    {
      "metadata": {
        "id": "019c6f7e-7f21-7a37-8cb6-91a4bc5fbef1",
        "revision": 41,
        "created_at": "2026-08-29T10:00:00Z",
        "updated_at": "2026-08-29T10:00:00Z"
      },
      "description": "Write the guide",
      "list": "in",
      "state": "pending",
      "labels": {}
    }
  ]
}
```

`PUT /api/v1/tasks/{id}` submits the complete target mutable state:

```json
{
  "metadata": { "revision": 41 },
  "description": "Write the guide; review the existing draft first",
  "list": "next-action",
  "state": "pending",
  "labels": { "project": "gtd", "place": "home" }
}
```

- `metadata.revision` must match the task's current event revision. A mismatch
  returns `409 Conflict` and writes no event. The client must GET the task again,
  merge the user's change, and decide whether to retry.
- `description`, `list`, `state`, and `labels` are required complete target values,
  not a patch. For example, `"labels": {}` clears every label. Add supplemental
  information by editing `description`.
- The ID comes from the URL. The server owns `created_at` and `updated_at`, so PUT
  metadata contains only `revision`.
- A description-only or label-only change may preserve `list/state`. An identical
  PUT returns `400 Bad Request` and appends no no-op event.
- On success, the server returns the complete Task with a new metadata revision and
  records a generic `task.updated` event. There is no transition-command field;
  unknown request fields are rejected.

The server does not interpret client operation intent. It only validates these
`list/state` changes:

| Task `list/state` before PUT | Target `list/state` in PUT |
| --- | --- |
| `in/pending` | `in/doing` |
| `next-action/pending` | `next-action/doing` |
| Any non-archive `*/doing` | `archive/done` |
| Any non-archive task | `archive/trash` |
| `in/pending` | `next-action/pending` |
| `in/pending` | `waiting-for/pending` |
| `in/pending`, `next-action/pending`, or `next-action/doing` | `someday-maybe/pending` |
| `someday-maybe/pending` | `next-action/pending` |

A before/after combination outside this table returns `409 Conflict`. The system
does not define a `TaskAction` model. CLI commands such as `pick` and `done` only
construct a target Task and submit the PUT above.

Server event types are limited to `task.created` and `task.updated`; client
operation names are never exposed. For live changes, both the SSE `id` and
`task.metadata.revision` in the payload are the database event revision. Regular
task-event data uses the same Task shape as list items:

```json
{
  "event_type": "task.updated",
  "task": {
    "metadata": {
      "id": "019c...",
      "revision": 42,
      "created_at": "2026-08-29T10:00:00Z",
      "updated_at": "2026-08-29T10:05:00Z"
    },
    "description": "Write the guide",
    "list": "archive",
    "state": "done",
    "labels": {}
  },
  "prev_task": {
    "metadata": {
      "id": "019c...",
      "revision": 41,
      "created_at": "2026-08-29T10:00:00Z",
      "updated_at": "2026-08-29T10:04:00Z"
    },
    "description": "Write the guide",
    "list": "next-action",
    "state": "doing",
    "labels": {}
  }
}
```

`task.created` omits `prev_task`; `task.updated` includes the complete `prev_task`.
Compacted and error control events retain their own formats.

Every optional field in a JSON structure uses omitempty semantics: a `None` value
omits the field instead of emitting `null`; a present value retains its normal
shape.

```bash
curl -N 'http://127.0.0.1:4040/api/v1/tasks?watch=true'
curl -N 'http://127.0.0.1:4040/api/v1/tasks?watch=true&revision=42'
```

`GET /api/v1/tasks`, `GET /api/v1/tasks/{id}`, and POST/PUT responses all include
`X-Revision`. Watch `revision` is inclusive and `Last-Event-ID` remains available
for resuming a stream; a query `revision` takes precedence. Watch replays from the
database, while in-memory notifications only reduce polling latency.

Watch accepts the same Task `list`, `state`, and AND-combined `labels` filters as
List. An update is emitted when either `task` or `prev_task` matches, so a client
also observes a task leaving its filtered set. Filters do not change revision
ordering: skipped events still advance the stream's internal replay cursor.

All public business APIs except `/health` use the `/api/v1` prefix.

The server binds only to loopback by default and currently has no authentication.
Do not expose it directly to the public internet without a reverse proxy and an
authentication layer.

Compaction has no HTTP administration endpoint. When writes are stopped, or when
there is exactly one confirmed maintainer, operate on the local database directly:

```bash
cargo run -- compact 10000 --database ./gtd.db
```

The command first advances the scheduled revision, deletes superseded historical
events in batches, and finally advances the finished revision. Each task's compact
anchor is retained.

## Architecture

```text
CLI / Ratatui TUI
        | HTTP + SSE
        v
    Axum Server
        | TaskRepository trait
        v
Diesel SQLite event repository
```

Axum depends only on the `TaskRepository` trait, not on Diesel types.
`SqliteRepository` owns the connection pool, event appends, revisions, database
watch replay, and compaction. Labels currently live in the event JSON as part of
complete task state. A dedicated arbitrary-label index remains a TODO; this design
does not add a projection.

## Development verification

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release
python3 benchmarks/api-performance/benchmark.py
```

`tests/watch_resilience.rs` uses real TCP/SSE connections to cover server restart,
watch-client restart, temporary network interruption, and compaction watermarks.
The deep-history API benchmark uses only temporary databases. See the current
[Watch, API performance, and compaction validation report](benchmarks/api-performance/report.md).
