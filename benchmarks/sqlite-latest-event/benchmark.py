#!/usr/bin/env python3
"""Isolated SQLite benchmark for latest task-event query strategies.

The benchmark depends only on Python's standard library. It creates synthetic
databases in a TemporaryDirectory and never imports application code or opens an
application database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any


DEFAULT_TASK_COUNTS = (1, 10, 100, 1_000, 10_000)
DEFAULT_VERSION_COUNTS = (1, 10, 100, 1_000)

SCHEMA_SQL = """
CREATE TABLE task_events (
    revision INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    prev_revision INTEGER,
    event_type TEXT NOT NULL,

    description TEXT NOT NULL,
    list_name TEXT NOT NULL,
    state TEXT NOT NULL,
    labels TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    prev_description TEXT,
    prev_list_name TEXT,
    prev_state TEXT,
    prev_labels TEXT,
    prev_updated_at TIMESTAMP
);

CREATE INDEX task_events_task_revision_idx
    ON task_events (task_id, revision DESC);

CREATE INDEX task_events_list_task_revision_idx
    ON task_events (list_name, task_id, revision DESC);

CREATE INDEX task_events_type_revision_idx
    ON task_events (event_type, revision DESC);

CREATE UNIQUE INDEX task_events_root_idx
    ON task_events (task_id)
    WHERE prev_revision IS NULL;
"""

GROUP_MAX_SQL = """
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
ORDER BY event.created_at ASC, event.task_id ASC
"""

LOOSE_SCAN_SQL = """
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
ORDER BY event.created_at ASC, event.task_id ASC
"""

INSERT_SQL = """
INSERT INTO task_events (
    revision, task_id, prev_revision, event_type,
    description, list_name, state, labels,
    created_at, updated_at,
    prev_description, prev_list_name, prev_state, prev_labels, prev_updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def parse_counts(raw: str) -> tuple[int, ...]:
    values = tuple(int(value.strip()) for value in raw.split(",") if value.strip())
    if not values or any(value < 1 for value in values):
        raise argparse.ArgumentTypeError("counts must be comma-separated positive integers")
    return tuple(sorted(set(values)))


def uuid_v7_text(task_number: int) -> str:
    """Return a deterministic, ordered, UUID-v7-shaped task ID."""
    timestamp = 0x019B76DAA000 + task_number
    prefix = f"{timestamp:012x}"
    return f"{prefix[:8]}-{prefix[8:]}-7000-8000-{task_number:012x}"


def rows_for_versions(
    task_ids: list[str], start_version: int, end_version: int
) -> Iterable[tuple[Any, ...]]:
    task_count = len(task_ids)
    timestamp = "2026-01-01 00:00:00.000000"
    labels = '{"suite":"sqlite-latest-event"}'
    description = "synthetic benchmark task"

    for version in range(start_version, end_version + 1):
        for offset, task_id in enumerate(task_ids, start=1):
            revision = (version - 1) * task_count + offset
            prev_revision = None if version == 1 else revision - task_count
            created = version == 1
            yield (
                revision,
                task_id,
                prev_revision,
                "task.created" if created else "task.updated",
                description,
                "in",
                "pending",
                labels,
                timestamp,
                timestamp,
                None if created else description,
                None if created else "in",
                None if created else "pending",
                None if created else labels,
                None if created else timestamp,
            )


def result_digest(rows: list[tuple[Any, ...]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def execute_query(
    connection: sqlite3.Connection, sql: str, read_revision: int
) -> list[tuple[Any, ...]]:
    return connection.execute(sql, {"read_revision": read_revision}).fetchall()


def calibrate_repetitions(elapsed_seconds: float, target_seconds: float) -> int:
    if elapsed_seconds <= 0:
        return 1_000
    return max(1, min(1_000, math.ceil(target_seconds / elapsed_seconds)))


def measure(
    connection: sqlite3.Connection,
    sql: str,
    read_revision: int,
    samples: int,
    target_sample_seconds: float,
) -> dict[str, Any]:
    start = time.perf_counter_ns()
    warm_rows = execute_query(connection, sql, read_revision)
    warm_elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
    repetitions = calibrate_repetitions(warm_elapsed, target_sample_seconds)

    timings_ms: list[float] = []
    observed_count = len(warm_rows)
    for _ in range(samples):
        start = time.perf_counter_ns()
        for _ in range(repetitions):
            rows = execute_query(connection, sql, read_revision)
            if len(rows) != observed_count:
                raise AssertionError("query row count changed during measurement")
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        timings_ms.append(elapsed_ms / repetitions)

    ordered = sorted(timings_ms)
    p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return {
        "median_ms": statistics.median(timings_ms),
        "min_ms": min(timings_ms),
        "p95_ms": ordered[p95_index],
        "samples": samples,
        "repetitions_per_sample": repetitions,
        "row_count": observed_count,
    }


def query_plan(connection: sqlite3.Connection, sql: str, read_revision: int) -> list[str]:
    rows = connection.execute(
        "EXPLAIN QUERY PLAN " + sql, {"read_revision": read_revision}
    ).fetchall()
    return [" | ".join(str(value) for value in row) for row in rows]


def database_bytes(connection: sqlite3.Connection) -> int:
    page_count = connection.execute("PRAGMA page_count").fetchone()[0]
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    return int(page_count) * int(page_size)


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    columns = (
        "tasks_n",
        "versions_v",
        "events_e",
        "database_bytes",
        "strategy",
        "median_ms",
        "min_ms",
        "p95_ms",
        "samples",
        "repetitions_per_sample",
        "row_count",
        "result_sha256",
    )
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows({column: record[column] for column in columns} for record in records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tasks",
        type=parse_counts,
        default=DEFAULT_TASK_COUNTS,
        help="comma-separated N values (default: 1,10,100,1000,10000)",
    )
    parser.add_argument(
        "--versions",
        type=parse_counts,
        default=DEFAULT_VERSION_COUNTS,
        help="comma-separated V values (default: 1,10,100,1000)",
    )
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--target-sample-seconds", type=float, default=0.10)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "results",
    )
    args = parser.parse_args()

    if args.samples < 1 or args.target_sample_seconds <= 0:
        parser.error("samples and target-sample-seconds must be positive")
    if max(args.tasks) > 10_000 or max(args.versions) > 1_000:
        parser.error("this suite is scoped to N <= 10000 and V <= 1000")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    plans: dict[str, dict[str, list[str]]] = {}
    started = time.time()

    with tempfile.TemporaryDirectory(prefix="sqlite-latest-event-") as temp_dir:
        for task_count in args.tasks:
            database_path = Path(temp_dir) / f"n-{task_count}.sqlite3"
            connection = sqlite3.connect(database_path)
            connection.executescript(
                "PRAGMA journal_mode = OFF;\n"
                "PRAGMA synchronous = OFF;\n"
                "PRAGMA locking_mode = EXCLUSIVE;\n"
                + SCHEMA_SQL
            )
            task_ids = [uuid_v7_text(number) for number in range(1, task_count + 1)]
            previous_version = 0

            for version_count in args.versions:
                insert_started = time.perf_counter()
                connection.execute("BEGIN")
                connection.executemany(
                    INSERT_SQL,
                    rows_for_versions(task_ids, previous_version + 1, version_count),
                )
                connection.commit()
                insert_seconds = time.perf_counter() - insert_started
                previous_version = version_count

                connection.execute("ANALYZE task_events")
                connection.execute("PRAGMA optimize")
                read_revision = task_count * version_count

                group_rows = execute_query(connection, GROUP_MAX_SQL, read_revision)
                loose_rows = execute_query(connection, LOOSE_SCAN_SQL, read_revision)
                group_digest = result_digest(group_rows)
                loose_digest = result_digest(loose_rows)
                if group_rows != loose_rows or len(group_rows) != task_count:
                    raise AssertionError(
                        f"result mismatch for N={task_count}, V={version_count}: "
                        f"group={len(group_rows)}/{group_digest}, "
                        f"loose={len(loose_rows)}/{loose_digest}"
                    )

                # Alternate measurement order to reduce a systematic first-query bias.
                strategies = [
                    ("group_max", GROUP_MAX_SQL),
                    ("loose_scan", LOOSE_SCAN_SQL),
                ]
                if (len(records) // 2) % 2:
                    strategies.reverse()

                measured: dict[str, dict[str, Any]] = {}
                for strategy, sql in strategies:
                    measured[strategy] = measure(
                        connection,
                        sql,
                        read_revision,
                        args.samples,
                        args.target_sample_seconds,
                    )

                size = database_bytes(connection)
                for strategy in ("group_max", "loose_scan"):
                    record = {
                        "tasks_n": task_count,
                        "versions_v": version_count,
                        "events_e": read_revision,
                        "database_bytes": size,
                        "strategy": strategy,
                        **measured[strategy],
                        "result_sha256": group_digest,
                    }
                    records.append(record)

                plan_key = f"N={task_count},V={version_count}"
                plans[plan_key] = {
                    "group_max": query_plan(connection, GROUP_MAX_SQL, read_revision),
                    "loose_scan": query_plan(connection, LOOSE_SCAN_SQL, read_revision),
                }
                write_csv(args.output_dir / "timings.csv", records)
                print(
                    f"N={task_count:>5} V={version_count:>4} "
                    f"E={read_revision:>8} build={insert_seconds:>7.2f}s "
                    f"group={measured['group_max']['median_ms']:>9.3f}ms "
                    f"loose={measured['loose_scan']['median_ms']:>9.3f}ms "
                    f"speedup={measured['group_max']['median_ms'] / measured['loose_scan']['median_ms']:>7.2f}x",
                    flush=True,
                )

            connection.close()

    environment = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_seconds": time.time() - started,
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "cpu_model": cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "tasks": args.tasks,
        "versions": args.versions,
        "samples": args.samples,
        "target_sample_seconds": args.target_sample_seconds,
        "database_lifecycle": "one temporary database per N; incrementally grown through V; deleted at exit",
        "cache_mode": "warm SQLite/OS cache; SQLite default cache_size and temp_store",
        "build_pragmas": {
            "journal_mode": "OFF",
            "synchronous": "OFF",
            "locking_mode": "EXCLUSIVE",
        },
    }
    (args.output_dir / "environment.json").write_text(
        json.dumps(environment, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "query-plans.json").write_text(
        json.dumps(plans, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"results: {args.output_dir}")


if __name__ == "__main__":
    main()
