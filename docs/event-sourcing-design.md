# Task Event Sourcing Design

- Status: Implemented
- Date: 2026-08-30
- Scope: SQLite `TaskRepository`, task HTTP API, SSE watch, revisions, and compaction

## 1. Summary

The database uses two tables. `task_events` stores only task events, while
`task_event_compaction` stores event-history compaction state. Every successful
task change inserts one row into `task_events`. Tasks are never updated in place,
and there are no separate `tasks` or `labels` projections.

The core design is:

- `task_events.revision` is an SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`;
- an event revision is the global ordering token;
- `task_id` is an application-generated UUID v7;
- unprefixed task fields contain the complete state after the change;
- corresponding `prev_` fields contain the state before the change;
- current task state is derived from the highest event revision for each `task_id`;
- watch uses revisions for ordering, replay, and resumption;
- the compaction table records scheduled and finished revisions separately;
- physical compaction retains the last event for each task at the watermark as an
  anchor.

The primary references are:

- the monotonic revision and MVCC history in the
  [etcd data model](https://etcd.io/docs/v3.6/learning/data_model/);
- the [etcd Watch API](https://etcd.io/docs/v3.6/learning/api/) and
  [watch guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/);
- the increasing row ID, previous revision, old value, and compact marker in the
  [Kine SQL log](https://github.com/k3s-io/kine/blob/master/pkg/drivers/generic/generic.go);
- the combination of database polling and local notification in the
  [Kine watch flow](https://github.com/k3s-io/kine/blob/master/docs/flow.md).

## 2. Goals and non-goals

### 2.1 Goals

- The final business schema contains only `task_events` and
  `task_event_compaction`.
- Every successful task change inserts exactly one event.
- An event contains complete task state after the change, mutable state before the
  change, and `prev_updated_at`.
- Event revisions increase monotonically for the lifetime of the database and
  committed values are never reused.
- Current state, historical state, and watch all read the same event table.
- Watch is ordered, reliable, and resumable within the uncompacted history window.
- Current task state remains reconstructable after compaction.

### 2.2 Non-goals

- Implementing Raft, replication, leader election, or etcd's distributed
  consistency model.
- Implementing the complete etcd KV/gRPC API.
- Maintaining a separate current-state projection.
- Allowing multiple events to share a revision; each event has its own revision.
- Providing exactly-once business processing; clients must process revisions
  idempotently.
- Exposing a point-in-time HTTP API in the first version.

## 3. Core semantics

### 3.1 Revision

`task_events.revision` is the global event revision for the database:

- the first event has revision 1;
- SQLite allocates a larger revision for every inserted event;
- one task change produces one event and one revision;
- temporary ROWIDs from rolled-back transactions are not committed revisions;
- after compaction deletes old events, SQLite `AUTOINCREMENT` still prevents reuse
  of committed revisions;
- a revision expresses only global ordering. Event occurrence time is not stored
  separately; task state time is represented by `created_at` and `updated_at`.

The current revision is:

```sql
SELECT COALESCE(MAX(revision), 0) AS current_revision
FROM task_events;
```

Compaction must never delete the event with the highest revision, so
`MAX(revision)` cannot move backward after physical cleanup.

### 3.2 Task ID

`task_id` is an application-generated UUID v7 and is independent of the event
revision. The database stores its canonical lowercase, hyphenated text form:

```text
019c6f7e-7f21-7a37-8cb6-91a4bc5fbef1
```

`TEXT` is used instead of a 16-byte `BLOB` so Diesel, operational SQL, the HTTP
API, and logs use one representation. Canonical UUID v7 strings retain their
time-prefix ordering.

The Rust UUID v7 generator must create the ID before entering the write transaction
or inside it. The database validates only the basic format. A Task ID is never
reused and does not depend on event count or compaction state.

### 3.3 Previous revision

`prev_revision` records the preceding event revision for the same task. It is
`NULL` for the creation event. This is sufficient to express the task event chain,
so no `task_version` is stored.

An event retains its original `prev_revision` even if the preceding event is later
compacted. There is intentionally no foreign key.

No runtime feature may query the preceding event through `prev_revision`.

### 3.4 Before and after fields

Events do not use a `before_state`/`after_state` JSON envelope.

- Unprefixed fields are the complete task state after the change.
- `prev_` fields are the corresponding state before the change.
- `created_at` is invariant for the lifetime of a task, so it is stored once and
  there is no `prev_created_at`.
- `updated_at` is the update time for the current event, and `prev_updated_at` is
  the update time before the change.

| After | Before |
| --- | --- |
| `description` | `prev_description` |
| `list_name` | `prev_list_name` |
| `state` | `prev_state` |
| `labels` | `prev_labels` |
| `updated_at` | `prev_updated_at` |

`created_at` has no before/after mapping. The before and after task states reuse
the current event row's invariant `created_at`.

A creation event has no previous state, so every `prev_*` field and
`prev_revision` is `NULL`. The presence of previous state is determined by
`prev_revision IS NOT NULL`, never by a nullable business field.

## 4. Schema

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE task_events (
    -- Global event revision
    revision            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,

    -- Task event envelope
    task_id             TEXT NOT NULL,
    prev_revision       INTEGER,
    event_type          TEXT NOT NULL CHECK (
        event_type IN ('task.created', 'task.updated')
    ),

    -- Complete task state after the change
    description         TEXT NOT NULL,
    list_name           TEXT NOT NULL,
    state               TEXT NOT NULL,
    labels              TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL,

    -- Task state before the change; created_at is invariant and is not duplicated
    prev_description    TEXT,
    prev_list_name      TEXT,
    prev_state          TEXT,
    prev_labels         TEXT,
    prev_updated_at     TIMESTAMP,

    CHECK (length(task_id) = 36),
    CHECK (prev_revision IS NULL OR prev_revision < revision),

    CHECK (
        list_name IN (
            'in',
            'next-action',
            'waiting-for',
            'someday-maybe',
            'archive'
        )
    ),
    CHECK (
        prev_list_name IS NULL OR
        prev_list_name IN (
            'in',
            'next-action',
            'waiting-for',
            'someday-maybe',
            'archive'
        )
    ),
    CHECK (state IN ('pending', 'doing', 'done', 'trash')),
    CHECK (
        prev_state IS NULL OR
        prev_state IN ('pending', 'doing', 'done', 'trash')
    ),
    CHECK (json_valid(labels)),
    CHECK (prev_labels IS NULL OR json_valid(prev_labels)),

    -- A NULL prev_revision means there is no previous state. Otherwise every
    -- previous-state field must be present.
    CHECK (
        (
            prev_revision IS NULL
            AND prev_description IS NULL
            AND prev_list_name IS NULL
            AND prev_state IS NULL
            AND prev_labels IS NULL
            AND prev_updated_at IS NULL
        )
        OR (
            prev_revision IS NOT NULL
            AND prev_description IS NOT NULL
            AND prev_list_name IS NOT NULL
            AND prev_state IS NOT NULL
            AND prev_labels IS NOT NULL
            AND prev_updated_at IS NOT NULL
        )
    ),

    -- Current task state follows the list/state domain constraint.
    CHECK (
        (list_name = 'archive' AND state IN ('done', 'trash'))
        OR (list_name <> 'archive' AND state IN ('pending', 'doing'))
    )
);

-- Latest state or complete history for one task
CREATE INDEX task_events_task_revision_idx
    ON task_events (task_id, revision DESC);

-- Current-list queries. Historical events only produce candidates; the query
-- must still verify that the row is the latest event for the task.
CREATE INDEX task_events_list_task_revision_idx
    ON task_events (list_name, task_id, revision DESC);

-- Watch and diagnostics by event type
CREATE INDEX task_events_type_revision_idx
    ON task_events (event_type, revision DESC);

CREATE TABLE task_event_compaction (
    singleton           INTEGER NOT NULL PRIMARY KEY
        CHECK (singleton = 1),
    scheduled_revision  INTEGER NOT NULL DEFAULT 0
        CHECK (scheduled_revision >= 0),
    finished_revision   INTEGER NOT NULL DEFAULT 0
        CHECK (
            finished_revision >= 0
            AND finished_revision <= scheduled_revision
        )
);

INSERT INTO task_event_compaction (
    singleton,
    scheduled_revision,
    finished_revision
) VALUES (1, 0, 0);
```

## 5. Field definitions

| Field | Meaning |
| --- | --- |
| `revision` | Event primary key and global revision |
| `task_id` | UUID v7 Task ID |
| `prev_revision` | Previous event revision for the same task |
| `event_type` | Stable resource-level event name |
| Unprefixed task fields | Complete task state after the change |
| `prev_*` task fields | Mutable state before the change, including `prev_updated_at`; there is no `prev_created_at` |

`task_event_compaction` fields:

| Field | Meaning |
| --- | --- |
| `singleton` | Fixed at 1 so there is exactly one compaction-state row |
| `scheduled_revision` | Effective logical compaction watermark; watch/history cannot read this revision or earlier |
| `finished_revision` | Watermark through which physical GC has completed |

Labels are stored as a JSON object:

```json
{
  "place": "home",
  "project": "gtd"
}
```

Serialization must use a stably ordered map. No labels are represented as `{}`,
not `NULL`. Labels belong directly to Task and are not nested in another object.

## 6. Event types

The first version defines:

| `event_type` | `task_id` | `prev_*` | Current task fields |
| --- | --- | --- | --- |
| `task.created` | Present | All `NULL` | Complete |
| `task.updated` | Present | Complete mutable state | Complete |

The server defines only resource-level create/update events. Client operation
intent is never encoded in the database or Watch. A database CHECK, Rust enum,
and Repository validation jointly limit `event_type` to `task.created` or
`task.updated`. A future resource-level event type changes the new schema directly;
compatibility with old databases is outside this design.

## 7. Current-state queries

### 7.1 List every latest task

Filters must apply to the latest event for each task. Filtering by list/state before
finding the maximum revision would incorrectly return an old event after a task
leaves that list or state.

Within one SQLite read transaction, List first reads current revision `R`. It then
uses a recursive loose index scan over `(task_id, revision DESC)`: jump to the next
distinct `task_id`, then find the first event for that task with `revision <= R`.

```sql
WITH RECURSIVE
task_ids(task_id) AS (
    SELECT MIN(task_id)
    FROM task_events

    UNION ALL

    SELECT (
        SELECT MIN(next.task_id)
        FROM task_events AS next
        WHERE next.task_id > task_ids.task_id
    )
    FROM task_ids
    WHERE task_id IS NOT NULL
),
latest_revisions(task_id, revision) AS MATERIALIZED (
    SELECT
        ids.task_id,
        (
            SELECT event.revision
            FROM task_events AS event
            WHERE event.task_id = ids.task_id
              AND event.revision <= :read_revision
            ORDER BY event.revision DESC
            LIMIT 1
        )
    FROM task_ids AS ids
    WHERE ids.task_id IS NOT NULL
)
SELECT event.*
FROM latest_revisions AS latest
CROSS JOIN task_events AS event
WHERE latest.revision IS NOT NULL
  AND event.revision = latest.revision
ORDER BY event.created_at ASC, event.task_id ASC;
```

`:read_revision` is a bind parameter in the implementation.

`AS MATERIALIZED` is an intentional optimization boundary. It prevents SQLite
from expanding the CTE and repeating each task's correlated latest-revision
subquery. `CROSS JOIN` fixes `latest_revisions` as the outer loop, so the final
lookup uses the `revision` INTEGER PRIMARY KEY instead of allowing some
`ORDER BY` plans to scan all of `task_events`.

An empty event log produces one internal `NULL task_id`; `latest_revisions` filters
that row, correctly returning an empty list at revision 0. Compaction retains an
anchor for each task, so the loose scan can still find every current task afterward.

### 7.2 List and state filters

The Repository selects one of two static SQL statements depending on whether a
list is present. This avoids `:list_name IS NULL OR ...`, which interferes with
SQLite index selection. With a list filter,
`(list_name, task_id, revision DESC)` first enumerates tasks that historically
belonged to the list, and `(task_id, revision DESC)` then finds each candidate's
latest event:

```sql
WITH RECURSIVE
candidate_task_ids(task_id) AS (
    SELECT MIN(task_id)
    FROM task_events
    WHERE list_name = :list_name

    UNION ALL

    SELECT (
        SELECT MIN(next.task_id)
        FROM task_events AS next
        WHERE next.list_name = :list_name
          AND next.task_id > candidate_task_ids.task_id
    )
    FROM candidate_task_ids
    WHERE task_id IS NOT NULL
),
latest_revisions(task_id, revision) AS MATERIALIZED (
    SELECT
        candidate.task_id,
        (
            SELECT event.revision
            FROM task_events AS event
            WHERE event.task_id = candidate.task_id
              AND event.revision <= :read_revision
            ORDER BY event.revision DESC
            LIMIT 1
        )
    FROM candidate_task_ids AS candidate
    WHERE candidate.task_id IS NOT NULL
)
SELECT event.*
FROM latest_revisions AS latest
CROSS JOIN task_events AS event
WHERE latest.revision IS NOT NULL
  AND event.revision = latest.revision
  AND event.list_name = :list_name
ORDER BY event.created_at ASC, event.task_id ASC;
```

The list predicate on historical events may only generate candidates. The outer
query must check `event.list_name` on the latest event to avoid false positives
for tasks that left the list. A task currently in the target list cannot be a false
negative because its latest event is itself a candidate.

State is not pushed into SQL and has no query index in the first version. After
converting complete latest-state rows to `Task`, the Repository filters in Rust:

```rust
if let Some(state) = filter.state {
    tasks.retain(|task| task.state == state);
}
```

A state-only request therefore performs the global loose scan. A combined
list/state request first narrows list candidates and then filters state in the
application. Every predicate applies to latest state, never historical state.

### 7.3 Get one task

```sql
SELECT *
FROM task_events
WHERE task_id = :task_id
ORDER BY revision DESC
LIMIT 1;
```

### 7.4 Label filters

The first version guarantees label-filter correctness but does not solve arbitrary
label indexing. The Repository gets the latest events matching the list through
the loose scan, filters state in Rust, parses each event's label JSON, and then
applies label filters. The semantics are equivalent to running this predicate over
the latest-event set:

```sql
EXISTS (
    SELECT 1
    FROM json_each(event.labels) AS label
    WHERE label.key = :key
      AND label.value = :value
)
```

Multiple label predicates have AND semantics. They must not be pushed into the
historical-event enumeration phase of the loose scan, or an old event would create
a false positive for a task that has since removed the label.

TODO(labels-index): design an arbitrary-label index separately, using real data
volume and query patterns. This change adds no `task_event_labels`, current-labels
table, or other projection, and makes no label-query performance commitment. Until
the TODO is resolved, use `json_each()` or application-level JSON filtering.

## 8. Write transactions

All domain writes use `BEGIN IMMEDIATE`. This prevents another writer from changing
the latest event between reading it, validating target state, and appending the
next event. HTTP updates compare-and-set the target state against the current task
revision.

### 8.1 Create

```text
BEGIN IMMEDIATE
  1. Generate task_id with UUID v7 and confirm that the ID has no existing event
  2. Build complete new state
  3. INSERT task.created
     prev_revision = NULL
     every prev_* = NULL
  4. Obtain revision from INSERT ... RETURNING
COMMIT
  5. Publish the revision notification
```

### 8.2 Conditional PUT update

```text
BEGIN IMMEDIATE
  1. Find the highest-revision event for task_id
  2. Hydrate its unprefixed fields as the before Task
  3. Validate request.revision == before.revision; otherwise return 409
  4. Validate the before and target list/state combination
  5. Validate and normalize the complete target description and labels; build after
  6. INSERT task.updated
     prev_revision = before.revision
     prev_* = complete before state
     unprefixed fields = complete after state
  7. RETURNING revision
COMMIT
  8. Publish the revision notification
```

The revision comparison and event append occur in the same `BEGIN IMMEDIATE`
transaction, so at most one of two writers holding the same revision can succeed.
The losing writer receives `409 Conflict` with expected/current revisions and must
read the task again before deciding whether to retry.

PUT `description/list/state/labels` are complete target state, not a patch.
`labels: {}` explicitly clears all labels. Supplemental information is added by
editing the description. A description-only or label-only change may preserve the
original list/state. Invalid state changes, no-op changes, revision conflicts, and
rolled-back transactions produce no committed event.

### 8.3 Repository return value

Repository write methods return the persisted event instead of returning only a
task and asking the API layer to infer the event:

```rust
struct MutationResult {
    event: TaskEvent,
}
```

Create and update return one event. The Task response is built directly from its
unprefixed fields, with the resource ID, event revision, and server-managed times
inside `Task.metadata`.

### 8.4 HTTP Task and PUT structures

Every create/get/list/update Task response uses one structure:

```rust
struct TaskMetadata {
    id: Uuid,            // UUID v7
    revision: i64,       // Event revision that produced the current state
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct Task {
    metadata: TaskMetadata,
    description: String,
    list: TaskList,
    state: TaskState,
    labels: BTreeMap<String, String>,
}

struct TaskListMetadata {
    revision: i64,       // Global snapshot watermark of the list transaction
}

struct TaskListResponse {
    metadata: TaskListMetadata,
    items: Vec<Task>,
}
```

`metadata.revision` is an event revision; no task version is added or restored.
The List body uses a `metadata + items` envelope. Each
`items[].metadata.revision` belongs to that task, while
`TaskListResponse.metadata.revision` is the read transaction's global snapshot
watermark. It is normal for the former to be lower when another task has a newer
event. The `X-Revision` response header carries the same global watermark.

PUT uses a complete writable-state structure:

```rust
struct UpdateTaskMetadata {
    revision: i64,       // Expected current Task.metadata.revision
}

struct UpdateTaskRequest {
    metadata: UpdateTaskMetadata,
    description: String, // Complete editable target value
    list: TaskList,      // Complete target value
    state: TaskState,    // Complete target value
    labels: BTreeMap<String, String>, // Complete replacement
}
```

The URL specifies `id`. The server owns `created_at` and `updated_at`, so PUT
metadata contains only revision. Every request field is required, and unknown
fields are rejected.

The system defines no `TaskAction` type or transition-command field. The Repository
validates only the state changes below, and every successful update produces
`task.updated`:

| Before Task `list/state` | PUT target Task `list/state` |
| --- | --- |
| `in/pending` | `in/doing` |
| `next-action/pending` | `next-action/doing` |
| Non-archive `*/doing` | `archive/done` |
| Any non-archive Task | `archive/trash` |
| `in/pending` | `next-action/pending` |
| `in/pending` | `waiting-for/pending` |
| `in/pending`, `next-action/pending`, or `next-action/doing` | `someday-maybe/pending` |
| `someday-maybe/pending` | `next-action/pending` |

For example, to move an `in/pending` Task read at revision 41 to
`next-action/pending`, submit:

```json
{
  "metadata": {"revision": 41},
  "description": "Write the guide; draft the outline first",
  "list": "next-action",
  "state": "pending",
  "labels": {"project": "gtd"}
}
```

The successful response is the same complete Task structure with a new event
revision in `metadata.revision`. A before/target combination outside the table
returns `409 Conflict`.

## 9. Consistency invariants

The implementation and tests must preserve:

1. Every successful task change inserts exactly one event.
2. `revision` is the unique event primary key and increases in committed insertion
   order.
3. A committed revision is never reused.
4. Event rows cannot be updated; only controlled compaction may delete them.
5. `task_id` is a valid UUID v7 and each task has exactly one root event with
   `prev_revision IS NULL`.
6. Every `prev_*` field on a root event is `NULL`.
7. A non-root event's `prev_revision` equals the preceding event revision for the
   same task, unless that event has been compacted.
8. Every `prev_*` value on a non-root event equals the corresponding unprefixed
   value on the preceding event.
9. Current task state equals the unprefixed fields of the highest-revision event
   for its `task_id`.
10. Revisions and `updated_at` never move backward for one task.
11. `finished_revision <= scheduled_revision <= current_revision`.
12. Compaction retains the last event for each task at the compaction watermark.
13. Compaction does not delete the highest-revision event in the table.

Database CHECK constraints cover only row-local invariants. The Repository and
tests enforce equality across adjacent states.

## 10. Watch

### 10.1 API

```http
GET /api/v1/tasks?watch=true&revision=123
Last-Event-ID: 122
```

Rules:

- `watch=true` changes `GET /api/v1/tasks` from a regular List to an SSE watch.
- `revision` is inclusive.
- The `revision` query parameter takes precedence over `Last-Event-ID`.
- `Last-Event-ID: R` resumes at `R + 1`.
- If neither value is present, read current revision R and start at `R + 1`.
- `revision <= scheduled_revision` returns `410 Gone`.
- `revision > current_revision + 1` returns `400 Bad Request`.
- Watch supports the same Task `list`, `state`, and AND-combined `labels` filters as
  List. No event-type or other Watch filters are defined.
- Supplying `revision` with `watch=false` returns `400 Bad Request`.

The SSE protocol field is named `id`; its value is the event revision. A regular
task-event payload uses the same Task structure as a List item and adds the event
type and complete previous state:

```text
id: 42
event: task.updated
data: {
  "event_type": "task.updated",
  "task": {
    "metadata": {
      "id": "019c6f7e-7f21-7a37-8cb6-91a4bc5fbef1",
      "revision": 42,
      "created_at": "2026-08-29T10:00:00Z",
      "updated_at": "2026-08-29T10:05:00Z"
    },
    "description": "Write the guide",
    "list": "archive",
    "state": "done",
    "labels": {"project":"gtd"}
  },
  "prev_task": {
    "metadata": {
      "id": "019c6f7e-7f21-7a37-8cb6-91a4bc5fbef1",
      "revision": 39,
      "created_at": "2026-08-29T10:00:00Z",
      "updated_at": "2026-08-29T10:04:00Z"
    },
    "description": "Write the guide",
    "list": "next-action",
    "state": "doing",
    "labels": {"project":"gtd"}
  }
}
```

`task.created` includes `event_type` and a complete `task`, but omits `prev_task`
through omitempty semantics. `task.updated` includes the complete `prev_task`.
Compacted and error messages are Watch control events and do not use the Task
envelope.

Every serializable JSON type follows the same rule: an `Option<T>` whose value is
`None` omits the field rather than emitting `"field": null`. A missing optional
field still deserializes to `None`.

### 10.2 Database replay

```sql
SELECT *
FROM task_events
WHERE revision >= :next_revision
ORDER BY revision ASC
LIMIT :batch_size;
```

The Repository reads only the event rows in the batch and never queries a previous
event through `prev_revision`. For a non-creation event, it builds `prev_task`
directly from that row: metadata revision uses `prev_revision`, created_at reuses
the invariant `created_at`, updated_at uses `prev_updated_at`, and the remaining
values use the corresponding `prev_*` fields. Watch can therefore return complete
previous state even if the preceding event has been compacted.

After a successful send, set `next_revision = event.revision + 1`.

### 10.3 Notification and polling

The in-process channel carries only a notification that the database has a new
revision; it never carries the event payload:

1. Subscribe to the notifier before establishing the watch read point.
2. Replay history from the database.
3. After catching up, wait for either the notifier or the polling timer.
4. If the channel reports lag, query the database again.
5. If a process exits after commit but before notification, polling still finds the
   event.
6. If another server process writes the same database, polling still finds the
   event.

The database is the only reliable queue. The memory channel is a low-latency hint.

### 10.4 Seamless List-to-Watch handoff

Within one SQLite read transaction, `GET /api/v1/tasks`:

1. reads `MAX(task_events.revision) = R`;
2. queries the latest event for every task;
3. returns `{"metadata":{"revision":R},"items":[...]}` and `X-Revision: R`.

The client reads R from `metadata.revision` and then requests
`GET /api/v1/tasks?watch=true&revision=R+1`. A concurrent write between List and
Watch cannot be missed.

### 10.5 Guarantees

Within the uncompacted window, Watch provides:

- revision ordering;
- no duplicates within one connection;
- resumption at one revision after the last successfully received event;
- no event loss due to memory-channel lag;
- database replay after server restart.

Business processing across connections remains at least once. A client should
persist a revision only after successful processing and deduplicate by revision.

### 10.6 Task filters

Watch applies `list`, `state`, and `labels` to both sides of each task change. An
event is emitted when either `task` or `prev_task` matches the complete filter. This
transition-aware rule sends both entry into and exit from the selected set, so a
client does not retain a stale task after its list, state, or labels change.

Events that match neither side are not serialized or sent, but their revisions
still advance the watcher's internal `next_revision`. Label predicates retain AND
semantics. Compacted and error control events are never suppressed by Task filters.

## 11. Compaction

### 11.1 Compaction watermarks

`task_event_compaction` records the effective logical watermark separately from
the completed physical-cleanup watermark:

```sql
SELECT scheduled_revision, finished_revision
FROM task_event_compaction
WHERE singleton = 1;
```

This mirrors etcd's scheduled and finished compact revisions in backend metadata:
old revisions become logically inaccessible first, physical history is deleted in
batches, and finished revision advances only after every batch completes. See the
[etcd compaction implementation](https://github.com/etcd-io/etcd/blob/main/server/storage/mvcc/kvstore_compaction.go).

To request compaction through C, run this in a `BEGIN IMMEDIATE` transaction:

```sql
UPDATE task_event_compaction
SET scheduled_revision = :target_revision
WHERE singleton = 1
  AND scheduled_revision <= :target_revision
  AND :target_revision <= (
      SELECT COALESCE(MAX(revision), 0)
      FROM task_events
  );
```

The request must satisfy:

```text
scheduled_revision <= C <= current_revision
```

After `scheduled_revision = C` commits, watch and history can no longer access
`revision <= C`. Compaction updates the independent state table, inserts no task
event, and consumes no event revision.

### 11.2 Physical garbage collection

When the background compactor finds
`finished_revision < scheduled_revision`, it resumes deleting events already
superseded by a later state at or below the scheduled watermark:

```sql
DELETE FROM task_events AS old
WHERE old.revision <= :scheduled_revision
  AND EXISTS (
      SELECT 1
      FROM task_events AS newer
      WHERE newer.task_id = old.task_id
        AND newer.revision > old.revision
        AND newer.revision <= :scheduled_revision
  );
```

The implementation adds a revision range or row limit so each transaction deletes
only a small batch. This rule retains the last event for each task at C as the
compact anchor. The anchor remains physically present but is not visible to watch
or history requests whose parameter is `revision <= C`.

After every deletion batch for target C completes, advance the finished watermark:

```sql
UPDATE task_event_compaction
SET finished_revision = :target_revision
WHERE singleton = 1
  AND finished_revision <= :target_revision
  AND :target_revision <= scheduled_revision;
```

If the process exits during GC, startup observes
`finished_revision < scheduled_revision` and resumes the unfinished scheduled
revision. As in etcd, finished revision advances only after all physical deletion
has completed.

GC must retain the highest-revision event in the table so current revision, as
reported by `MAX(revision)`, never moves backward.

### 11.3 Slow watchers

Before each database replay, check compaction state. If
`next_revision <= scheduled_revision`:

1. send a terminating `compacted` message;
2. include `scheduled_revision` and `current_revision`;
3. close the connection;
4. require the client to List again and Watch from the new
   `metadata.revision + 1`.

### 11.4 Retention and space reclamation

Automatic compaction is disabled by default in the first version. A later policy
may choose a target based on the number of retained revisions. Events have no
independent occurrence timestamp, so this design does not provide time-based event
retention.

Deleting SQLite rows normally adds pages to the freelist without immediately
shrinking the database file. Full `VACUUM` or incremental vacuum is a separate
maintenance action. It produces no task event and changes no compaction state.

If permanent audit history is required, physical GC must be disabled or events
must be archived before compaction. Old events in backups do not disappear when
the primary database is compacted.

## 12. Historical reads and reconstruction

### 12.1 Task state at revision R

When `R > scheduled_revision`:

```sql
SELECT *
FROM task_events
WHERE task_id = :task_id
  AND revision <= :revision
ORDER BY revision DESC
LIMIT 1;
```

The compact anchor provides the correct baseline even if the task has not changed
since compaction.

### 12.2 Reconstructing the current database

The database has no current-state projection. Current state is always derived from
the highest event revision for every `task_id`.

A validation command may check:

- whether `prev_revision` names the preceding uncompacted event for the same task;
- whether a non-creation event's `prev_*` values equal the preceding event's
  current fields;
- whether latest events satisfy task list/state constraints;
- whether scheduled and finished compaction revisions remain monotonic and ordered.

These are offline validation checks only. Runtime features must not load a
preceding event through `prev_revision`.

## 13. Database initialization and compatibility

This redesign is incompatible with the old `tasks + labels` schema. It provides no
data migration and no mapping from old integer Task IDs. The first Diesel migration
defines the final `task_events + task_event_compaction` schema directly. Deployment
must use a new database or perform an operational export and re-import outside this
design.

The code retains no dual writes, old-table detection, old API, or compatibility
queries. The final business schema contains only `task_events`,
`task_event_compaction`, and Diesel's own migration records.

## 14. Rust types

```rust
struct TaskEvent {
    revision: i64,
    task_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_revision: Option<i64>,
    event_type: TaskEventType,

    description: String,
    list: TaskList,
    state: TaskState,
    labels: BTreeMap<String, String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,

    #[serde(skip_serializing_if = "Option::is_none")]
    prev_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_list_name: Option<TaskList>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_state: Option<TaskState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_labels: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_updated_at: Option<DateTime<Utc>>,
}

struct TaskEventCompaction {
    scheduled_revision: i64,
    finished_revision: i64,
}

struct TaskWatchEvent {
    event_type: TaskEventType,
    task: Task,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_task: Option<Task>,
}
```

`TaskEvent::task()` constructs after-state Task from unprefixed fields.
`TaskEvent::prev_task()` constructs before state from the same event row only:
revision comes from `prev_revision`, created_at from invariant `created_at`,
updated_at from `prev_updated_at`, and the remaining values from corresponding
`prev_*` fields. No feature may use `prev_revision` as a condition for loading the
preceding event.

## 15. Failure and concurrency

### 15.1 Concurrent writes

Create, update, and compact all use `BEGIN IMMEDIATE`:

- UUID v7 is generated in the application. Create confirms in the write
  transaction that the Task ID has no event and regenerates on an extreme collision.
- The latest event read by update cannot change before commit. Body-revision
  comparison and event append occur in the same transaction.
- Autoincrement revisions preserve the global order of multiple events.
- The existing `busy_timeout` handles brief lock contention.

### 15.2 Lost post-commit notification

Notification is sent after commit. If a process exits after commit but before
notify, the event remains in the database and watch polling recovers it. Notify
failure increases latency but does not lose data.

### 15.3 Slow consumers

Memory-channel lag never skips a database event. A watcher queries again from its
last successfully sent revision. If the required revision has been compacted, the
stream terminates explicitly instead of silently continuing.

### 15.4 Interrupted compaction

Scheduled revision commits before GC:

- if scheduled revision advances but GC has not started, history is already
  unreadable and only extra disk space remains;
- if the process exits after partial GC, startup sees
  `finished_revision < scheduled_revision` and resumes deletion;
- `finished_revision` advances only after all GC batches finish;
- repeating compaction at the same watermark succeeds idempotently;
- scheduled and finished revisions never move backward.

## 16. Performance impact

A single event log simplifies writes and historical ordering but makes current
queries more expensive than a projection:

- current-state List uses a recursive loose index scan with expected cost around
  `O(N log E)`, where `N` is task count and `E` is event count; final sorting adds
  about `O(N log N)`;
- list filtering enumerates the `C` distinct tasks that have historically entered
  the target list, at expected cost around `O(C log E)`; in the worst case `C=N`;
- application-level state filtering does not reduce SQL enumeration or row lookup;
- arbitrary label filtering requires JSON scanning;
- as event history grows, B-tree height, index size, and cache hit rate still
  change, so query time only approaches independence from task version count and
  is not strictly independent of it;
- compaction is required for stable long-term query performance;
- each event stores both current and `prev_*` fields, roughly doubling state data.

The isolated SQLite benchmark and complete results are in the
[SQLite latest-event query benchmark](../benchmarks/sqlite-latest-event/report.md).
It covers `N=1..10,000` and `V=1..1,000`. `GROUP BY + MAX` is about 20%-35% faster
at `V=1`; the approaches are close near `V=10`; and loose scan is
`12.48x-60.16x` faster at `V=1,000`. On the 10-million-event
`N=10,000,V=1,000` dataset, median time falls from `1,068.29 ms` to `64.45 ms`.
The implementation selects loose scan to optimize the lower bound when history is
deep.

Production should continue measuring:

- task count;
- average events per task;
- list/filter latency;
- watch backlog replay throughput;
- page and freelist counts before and after compaction.

If loose scan remains a bottleneck, a rebuildable projection can be introduced
later. That is an explicit architecture change outside the first version.

## 17. Test requirements

At minimum, cover:

- event revisions increase strictly and serve as API revisions;
- revisions are not reused after compaction deletes old rows;
- every creation `prev_*` and `prev_revision` is `NULL`;
- every update `prev_*` equals the preceding event's corresponding unprefixed field;
- update `prev_updated_at` equals the preceding event's `updated_at`, while previous
  created_at reuses the current event's invariant `created_at`;
- unprefixed update fields contain complete new state, and PUT labels replace all
  previous labels;
- a description-only update can retain list/state and records `prev_description`;
- create returns `task.created`, and every valid PUT returns `task.updated`;
- Server and Watch events contain no client operation name or action field;
- a Watch task event uses `event_type/task/prev_task`, with Task matching a List item;
- a created event omits `prev_task`, while an updated event includes complete
  previous metadata;
- every optional JSON field is omitted when its value is `None`;
- every valid before/target Task state passes validation;
- a stale revision returns conflict and inserts no event;
- invalid target state inserts no event;
- create generates valid, distinct UUID v7 Task IDs;
- concurrent writers produce neither duplicate root events nor duplicate revisions;
- current-state queries return only each task's highest-revision event;
- loose scan works for an empty event log and multiple tasks;
- historical list conditions only produce candidates and are rechecked on latest
  state;
- state filters run against latest Task in the application layer;
- multiple label conditions retain AND semantics;
- Watch list/state/label filters emit events whose before or after Task matches and
  skip events whose two sides do not match;
- watch can replay from any uncompacted revision;
- lost notification, channel lag, and server restart do not lose events;
- List `metadata.revision` and same-valued `X-Revision` connect to Watch without a
  gap;
- compaction advances scheduled revision without consuming an event revision;
- interrupted GC does not advance finished revision early and resumes on restart;
- compaction retains each task's anchor at the watermark;
- a compacted Watch revision returns an explicit error;
- Watch resumes after server restart from persisted revision without loss;
- events committed during Watch-client restart or temporary TCP interruption replay
  completely;
- an active Watch beyond the compact watermark keeps receiving later events, while
  a lagging Watch receives `410 Gone`;
- physical compaction deletes superseded events, retains each task anchor, and does
  not change the current List result;
- with automatic compaction disabled, the server does not advance scheduled or
  finished revision by itself;
- on an isolated dataset with at least 1,001 tasks and at least 101 versions per
  task, each local sample for every public API remains below one second;
- a newly initialized database has no `tasks` or `labels` table.

Real-TCP Watch integration tests are in `tests/watch_resilience.rs`. The deep-history
API benchmark, raw results, and limitations are under
`benchmarks/api-performance/`; its temporary databases are fully isolated from
application data.

## 18. Observability and security

Recommended measurements include:

- current event revision;
- scheduled compaction revision;
- finished compaction revision;
- total event count;
- average events per task;
- event append latency;
- current-state List latency;
- watch connection count, backlog, and replay rate;
- compacted row count and duration;
- SQLite page count, freelist count, and WAL size.

Do not print complete current fields or `prev_*` fields in regular logs.
Descriptions and labels may contain sensitive data.

Complete history retains information removed from current tasks. Compaction
retention, backup retention, and future privacy deletion must be explicit product
data policies, not merely performance parameters.

The current server has no authentication, so compaction, VACUUM, and history export
must not be exposed directly as public HTTP administration APIs. The first version
uses a local CLI or an internal Repository call.

## 19. Recorded decisions

1. The final business database has `task_events` and `task_event_compaction`.
2. Compaction state is not written to `task_events`.
3. There is no `tasks` or `labels` current-state projection.
4. The autoincrement `task_events.revision` primary key is the revision.
5. Each event has one revision; there is no main revision plus `event_index`.
6. Task ID is an application-generated UUID v7 stored as canonical text.
7. There is no task version; the event chain uses `prev_revision`.
8. There is no revisit field or automatic revisit feature.
9. The stored event envelope contains only `revision`, `task_id`, `prev_revision`,
   and `event_type`.
10. `event_type` expresses only resource-level `task.created`/`task.updated`, not
    client operation intent.
11. After state uses the original task field names.
12. Before state uses a `prev_` prefix on each mutable field.
13. Labels are stored as a JSON object in the event row.
14. `task_event_compaction` stores scheduled and finished revisions separately.
15. The database is the reliable Watch source; memory notification only wakes it.
16. Label indexing is deferred under `TODO(labels-index)`; this change adds no
    projection.
17. Current-state List uses a recursive loose index scan over
    `(task_id, revision DESC)`.
18. A list condition first narrows candidates with
    `(list_name, task_id, revision DESC)` and is rechecked on the latest event;
    state is filtered in the application layer.
19. There is no compatibility or migration path for the old database or integer
    Task IDs.
20. The system has no `TaskAction`; HTTP PUT submits complete target
    `description/list/state/labels`.
21. Task response metadata contains ID, the event revision that produced current
    state, created_at, and updated_at. PUT metadata uses revision for compare-and-set
    and returns `409 Conflict` on mismatch.
22. Labels live directly on Task and PUT replaces them completely. There is no note;
    edit description for supplemental information. The server manages timestamps.
23. The optimistic-concurrency token is in the JSON body; the first version does
    not support `If-Match`.
24. Events do not store `prev_created_at` because `created_at` is invariant. Events
    do store `prev_updated_at`, and `TaskEvent::prev_task()` constructs complete
    previous state from the current row only.
25. List responses use a `metadata + items` envelope. List metadata revision is the
    global snapshot watermark; each item has its own Task metadata revision.
26. Updated Watch events use `event_type/task/prev_task`; created events omit
    `prev_task`. The Repository never reads a preceding event through
    `prev_revision`. Both task and prev_task use the List-item structure.
27. Every serializable optional field uses omitempty semantics when its value is
    `None`.
28. Watch supports only Task `list`, `state`, and `labels` filters. An event is sent
    if either its before or after Task matches, preserving entry and exit changes.

## 20. Open parameters

These parameters can be selected during implementation without changing the core
schema:

1. watch polling interval and event batch size;
2. whether automatic compaction remains disabled or uses a conservative retention
   policy;
3. whether to expose a Task history HTTP API;
4. whether to archive externally before compaction.

The safe defaults are: automatic compaction disabled, only task-event Watch
exposed, and no public compaction HTTP API. Task bodies use task event revisions;
create/get/list/update responses also expose the corresponding revision or read
watermark through `X-Revision`.
