# SQLite Latest Task-Event Query Performance Report

- Test date: 2026-09-01
- SQLite: 3.46.1 through the Python standard-library binding
- Python: 3.14.4
- Platform: Linux x86_64, 16 logical CPUs
- Test range: `N = 1..10,000`, `V = 1..1,000`
- Raw data: [results/timings.csv](results/timings.csv)
- Query plans: [results/query-plans.json](results/query-plans.json)
- Environment: [results/environment.json](results/environment.json)

## 1. Conclusions

Without adding a current-state projection, and using only `task_events` plus the
`(task_id, revision DESC)` index, a recursive loose index scan changes the dominant
query cost from total event count `E = N x V` toward task count `N`.

Measured results:

- At `V = 1`, `GROUP BY + MAX` is about 20%-35% faster because sequential
  aggregation has a lower constant cost when each task has only one version.
- At `V = 10`, the two approaches are approximately equal.
- At `V = 100`, loose scan is `1.93x-4.14x` faster.
- At `V = 1,000`, loose scan is `12.48x-60.16x` faster.
- On the largest dataset, `N=10,000,V=1,000` with 10 million events, median time
  falls from `1,068.29 ms` to `64.45 ms`, a `16.58x` improvement; p95 falls from
  `1,070.52 ms` to `65.85 ms`.

Loose scan therefore approaches independence from Task version count, but its time
is not strictly independent of `V`. At fixed `N=10,000`, increasing `V` by 1,000x
increases loose-scan median time from `16.88 ms` to `64.45 ms`, about `3.82x`, due
to B-tree height, database size, and cache hit-rate changes. The aggregation query
increases from `12.61 ms` to `1,068.29 ms`, about `84.75x`.

The current Repository uses loose scan to improve the worst case for unfiltered
List with deep history. If compaction leaves nearly every task with one event,
aggregation remains simpler and slightly faster, but that does not offset its
lower-bound degradation as history grows.

## 2. SQL under test

Both directly executable queries are also stored in [queries.sql](queries.sql).
Within the same read transaction, callers should resolve API `revision=0` to the
current revision:

```sql
SELECT COALESCE(MAX(revision), 0) AS read_revision
FROM task_events;
```

Bind the result to `:read_revision` in either query below. The benchmark binds the
dataset's current maximum revision. Revision-resolution time is excluded from the
query measurement.

### 2.1 Approach A: `GROUP BY + MAX`

```sql
WITH latest_revisions(task_id, revision) AS MATERIALIZED (
    SELECT task_id, MAX(revision)
    FROM task_events
    WHERE revision <= :read_revision
    GROUP BY task_id
)
SELECT event.*
FROM latest_revisions AS latest
CROSS JOIN task_events AS event
WHERE event.revision = latest.revision
ORDER BY event.created_at ASC, event.task_id ASC;
```

This approach processes eligible events through the
`(task_id, revision DESC)` index and aggregates by `task_id`. Its core work grows
with the number of eligible events.

### 2.2 Approach B: recursive loose index scan

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

For each task, this approach performs three index operations: seek the next
`task_id`, find the first event no newer than the read revision, and load the
complete event by revision primary key. Expected complexity is approximately
`O(N log E)`, with another `O(N log N)` for final sorting. Neither operation scans
all versions of an individual task.

`AS MATERIALIZED` prevents SQLite from expanding the CTE and repeatedly evaluating
the correlated subquery. `CROSS JOIN` fixes `latest_revisions` as the outer loop and
loads events by revision primary key, preventing query plans that rescan the event
table.

## 3. Test isolation

Benchmark code is completely isolated from application code:

- Tests exist only under `benchmarks/sqlite-latest-event/`.
- They use only the Python standard library and neither import nor execute anything
  under `src/`.
- They do not read configuration or discover, connect to, or modify any application
  database.
- Each `N` creates a new synthetic SQLite database in an operating-system temporary
  directory.
- The same temporary database grows incrementally from `V=1` through
  `V=10,100,1,000`.
- Every temporary SQLite database is deleted when the process exits.
- The repository retains only scripts, SQL, query plans, environment information,
  and timing CSV files.
- The complete run confirmed that all temporary databases were removed.

The test schema retains the complete after/`prev_*` event payload relevant to the
current query and all four task-event indexes from the migration, including
`(list_name, task_id, revision DESC)` for List candidate queries. Both unfiltered
benchmark statements use only `(task_id, revision DESC)`. The fixture omits the
compaction table, CHECK constraints, and append-only trigger because they do not
participate in read-only query plans. Fixture-only settings `journal_mode=OFF`,
`synchronous=OFF`, and `locking_mode=EXCLUSIVE` reduce data-generation time; they
are outside measured query time and do not change the SELECT statements under test.

## 4. Method

Complete command:

```sh
python3 benchmarks/sqlite-latest-event/benchmark.py
```

The matrix uses representative boundary and order-of-magnitude points:

- Task count `N`: `1, 10, 100, 1,000, 10,000`.
- Versions per Task `V`: `1, 10, 100, 1,000`.
- Total events `E = N x V`, up to 10 million.
- Events are inserted in version round-robin order, with exactly `V` events per
  task.
- `task_id` uses a deterministic, lexically ordered UUID-v7-shaped string.
- Both queries return complete events and apply the same `created_at, task_id` sort.
- Each combination runs a correctness check, warm-up, and then seven samples.
- Fast queries repeat adaptively inside one sample to target at least 100 ms per
  sample.
- Tables report medians; the raw CSV also stores min, p95, and repetitions per
  sample.
- Query execution order alternates to reduce fixed ordering bias.
- Tests use one process, no concurrency, warm cache, and SQLite's default
  `cache_size` and `temp_store`.

Correctness validation compares more than row counts. For every combination, it
compares the complete event result row by row and records a result SHA-256. All 20
combinations are identical between queries and each returns exactly `N` rows.

## 5. Complete results

Every value below is median time per query in milliseconds. Parentheses show
`GROUP BY + MAX / loose scan`; a value greater than `1x` means loose scan is faster.

| N | V=1 | V=10 | V=100 | V=1,000 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0042 / 0.0050 (`0.83x`) | 0.0034 / 0.0042 (`0.82x`) | 0.0081 / 0.0042 (`1.93x`) | 0.0520 / 0.0042 (`12.48x`) |
| 10 | 0.0110 / 0.0144 (`0.76x`) | 0.0173 / 0.0162 (`1.07x`) | 0.0628 / 0.0166 (`3.78x`) | 0.5187 / 0.0171 (`30.39x`) |
| 100 | 0.0946 / 0.1286 (`0.74x`) | 0.1531 / 0.1431 (`1.07x`) | 0.6240 / 0.1506 (`4.14x`) | 9.3684 / 0.1557 (`60.16x`) |
| 1,000 | 0.9844 / 1.3390 (`0.74x`) | 1.5997 / 1.5810 (`1.01x`) | 9.4111 / 3.9151 (`2.40x`) | 102.1144 / 4.6744 (`21.85x`) |
| 10,000 | 12.6055 / 16.8775 (`0.75x`) | 27.0008 / 27.2007 (`0.99x`) | 109.8155 / 52.6621 (`2.09x`) | 1,068.2926 / 64.4501 (`16.58x`) |

The SQLite file for the largest dataset is approximately `4,859,416,576` bytes
(`4.53 GiB`), including before/after event payloads and four indexes.

## 6. Query-plan validation

The key steps for Approach A on the largest dataset are:

```text
SEARCH task_events USING COVERING INDEX task_events_task_revision_idx
    (ANY(task_id) AND revision<?)
SCAN latest
SEARCH event USING INTEGER PRIMARY KEY (rowid=?)
USE TEMP B-TREE FOR ORDER BY
```

The key steps for Approach B are:

```text
SEARCH next USING COVERING INDEX task_events_task_revision_idx (task_id>?)
SEARCH event USING COVERING INDEX task_events_task_revision_idx
    (task_id=? AND revision<?)
SCAN latest
SEARCH event USING INTEGER PRIMARY KEY (rowid=?)
USE TEMP B-TREE FOR ORDER BY
```

This confirms that loose scan does not degrade to a full table scan during the
final event lookup. Both approaches must sort `N` result rows, so at `V=1` the
additional recursion and index seeks in loose scan are pure overhead.

## 7. Applicability limits

- This report measures the lower bound without list/state/label filters. Current
  filters must run after selecting each task's latest event, so a smaller filtered
  result does not reduce the cost of enumerating tasks in the first phase.
- Tests target the current read point after resolving `revision=0`. Older historical
  revisions and event distributions after compaction require separate measurement.
- A warm-cache microbenchmark is suitable for comparing algorithms and growth with
  `N/V`, but it is not production end-to-end latency. Disk, memory, concurrent
  writes, the connection pool, and Diesel mapping change absolute values.
- Python's SQLite 3.46.1 may not match the exact patch version of bundled SQLite in
  the application build. Before release, rerun the matrix with the final bundled
  SQLite.
- Loose scan is SQLite-specific and relatively complex. Retain
  `EXPLAIN QUERY PLAN` checks and performance regression tests when upgrading
  SQLite.
- Strict `O(N)` current-state reads independent of history depth require a
  current-state/head projection. This design explicitly avoids that projection, so
  it can only approximate independence from `V`.
