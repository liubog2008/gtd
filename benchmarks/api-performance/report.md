# Watch, API Performance, and Compaction Validation Report

- Test date: 2026-09-01
- Program under test: release `gtd` server over real loopback TCP/HTTP/SSE
- Watch/compaction tests: Rust integration tests with a real Axum TCP listener
- Performance dataset: 1,024 tasks x 101 versions = 103,424 preloaded events
- Sampling: one warm-up followed by seven measured samples per operation
- Acceptance threshold: end-to-end time `< 1,000 ms` for every sample
- Raw data: [timings.csv](results/timings.csv) and
  [environment.json](results/environment.json)

## 1. Conclusions

Every Watch recovery test passed, with no observed event loss:

- The server crashed after the watcher acknowledged only revision 1. After restart,
  `Last-Event-ID: 1` replayed revisions 2, 3, and 4 in order, followed by newly
  committed revision 5.
- A Watch client exited after reading revisions 1 and 2. A replacement client
  started at persisted revision 3 and received revisions 3 through 6 in order.
- Revisions 2 through 4 were committed during a temporary TCP interruption. After
  reconnecting with `Last-Event-ID: 1`, the client received all three.
- All three scenarios were strictly increasing by revision, without gaps or
  duplicates.

Every measured HTTP API sample completed in less than one second. Filtered List was
the slowest operation, with a maximum of `8.90 ms`, leaving approximately `112x`
headroom against the one-second threshold.

The current server does **not** run automatic periodic compaction. This is an
explicit default, not a test failure. Production code has no periodic scheduler or
retention parameter; compaction requires `gtd compact <revision>`. The integration
test confirms that server runtime alone does not advance compaction watermarks.

Explicit compaction affects Watch as designed. A new Watch at or below the
watermark returns `410 Gone`; an active Watch already at the watermark and a Watch
starting at the following revision both continue. Current Task state remains intact.

## 2. Watch recovery tests

The test code is in
[`tests/watch_resilience.rs`](../../tests/watch_resilience.rs). It uses real TCP
connections and parses SSE directly rather than bypassing the network with Axum
`Router::oneshot`.

| Scenario | Operation | Expected result and outcome |
| --- | --- | --- |
| Server restart | Stop the server after R1; start a new server on the same SQLite database; send `Last-Event-ID: 1` | Receive R2-R4 and then new R5; passed |
| Client restart | First client persists R2 and exits; second starts at `revision=3` | Receive R3-R6; passed |
| Temporary network interruption | Drop the TCP socket after R1; commit R2-R4 while disconnected | Receive R2-R4 after reconnect; passed |
| Compacted revision | Compact through R3, then start Watch at `revision=3` | HTTP `410 Gone`; passed |
| Active Watch across compaction | Watch has read R3; compact through R3; commit R4 and R5 | Same connection receives R4 and R5; passed |
| Retained window | Start at R4 after compacting through R3 | Replay R4 and R5; passed |
| Advance compaction again | Compact through R5 while a Watch waits for R6, then commit R6 | Receive R6; passed |

These tests validate the foundation for at-least-once recovery. A client should
persist its last revision only after successful business processing and deduplicate
idempotently by revision after reconnecting.

## 3. API performance results

| API | Median (ms) | P95 (ms) | Max (ms) | `<1s` |
| --- | ---: | ---: | ---: | :---: |
| `GET /health` | 0.21 | 0.25 | 0.25 | Yes |
| `GET /api/v1/tasks/{id}` | 0.25 | 0.29 | 0.29 | Yes |
| `GET /api/v1/tasks` | 5.77 | 6.02 | 6.02 | Yes |
| `GET /api/v1/tasks?list=in` | 8.24 | 8.90 | 8.90 | Yes |
| `GET /api/v1/tasks?state=pending` | 5.79 | 7.42 | 7.42 | Yes |
| `GET /api/v1/tasks?labels=...` | 6.10 | 6.36 | 6.36 | Yes |
| `GET /api/v1/tasks?list+state+labels` | 8.18 | 8.37 | 8.37 | Yes |
| `POST /api/v1/tasks` | 0.42 | 0.56 | 0.56 | Yes |
| `PUT /api/v1/tasks/{id}` | 0.45 | 0.51 | 0.51 | Yes |
| Watch replay, 1 event | 0.32 | 0.40 | 0.40 | Yes |
| Watch replay, 256 events | 1.65 | 1.72 | 1.72 | Yes |

Timing includes the TCP connection, HTTP, Axum, Diesel, SQLite, JSON/SSE
serialization, and the client reading the complete response. Every regular HTTP
sample opens a new connection, so passing does not depend on keep-alive.

## 4. Compaction validation

The test first produces R1 through R3 for one task and then runs `compact(3)`:

- `scheduled_revision = finished_revision = 3`;
- superseded events R1 and R2 are physically deleted;
- R3 remains as the task's compact anchor;
- Watch requests with `revision <= 3` are logically rejected;
- the current Task List result is unchanged;
- compaction produces no task event and does not change the highest current event
  revision;
- an active Watch beyond the watermark continues to receive later events.

Automatic periodic compaction did not run because that production feature does not
exist. Enabling it requires at least an execution interval and a retained-revision
count. A timer must not simply compact through current revision, because that would
immediately remove the recovery window for every briefly disconnected Watch.

## 5. Isolation and limitations

Performance code lives under `benchmarks/api-performance/`. It creates databases
only in operating-system temporary directories, removes them on exit, and never
reads or modifies an application database. SQL generates deep history directly
because the current GTD state machine is finite and one-way and cannot naturally
produce 101 valid business transitions per task through the public API. The
queries, Repository, server, and HTTP/SSE stack under test are still the release
application code. Fixtures use the current flat Task schema: labels are top-level,
and PUT submits complete description/list/state/labels.

These results are a single-machine, loopback, warm-cache regression baseline
without concurrent load; they are not a production SLO. Disk latency, concurrent
writers/watchers, cold cache, reverse proxies, and network RTT require load testing
in the target environment. The script primarily prevents a code change from
regressing the 103,424-event baseline beyond one second.
