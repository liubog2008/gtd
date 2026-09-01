#!/usr/bin/env python3
"""End-to-end API performance benchmark on a deep synthetic event history."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
RESULTS = Path(__file__).resolve().parent / "results"
DEFAULT_BINARY = ROOT / "target" / "release" / "gtd"
TIMESTAMP = "2026-08-31 00:00:00.000000"
LABELS = '{"bucket":"seed","suite":"api-performance"}'


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=1_024)
    parser.add_argument("--versions", type=int, default=101)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--threshold-ms", type=float, default=1_000.0)
    parser.add_argument("--server-binary", type=Path, default=DEFAULT_BINARY)
    return parser.parse_args()


def uuid_v7_text(task_number: int) -> str:
    timestamp = 0x019B76DAA000 + task_number
    prefix = f"{timestamp:012x}"
    return f"{prefix[:8]}-{prefix[8:]}-7000-8000-{task_number:012x}"


def rows(task_count: int, version_count: int) -> Iterable[tuple[Any, ...]]:
    task_ids = [uuid_v7_text(number) for number in range(1, task_count + 1)]
    for version in range(1, version_count + 1):
        for offset, task_id in enumerate(task_ids, start=1):
            revision = (version - 1) * task_count + offset
            root = version == 1
            yield (
                revision,
                task_id,
                None if root else revision - task_count,
                "task.created" if root else "task.updated",
                f"synthetic task {offset}",
                "in",
                "pending",
                LABELS,
                TIMESTAMP,
                TIMESTAMP,
                None if root else f"synthetic task {offset}",
                None if root else "in",
                None if root else "pending",
                None if root else LABELS,
                None if root else TIMESTAMP,
            )


INSERT_SQL = """
INSERT INTO task_events (
    revision, task_id, prev_revision, event_type,
    description, list_name, state, labels,
    created_at, updated_at,
    prev_description, prev_list_name, prev_state, prev_labels, prev_updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class Server:
    def __init__(self, binary: Path, database: Path) -> None:
        self.binary = binary
        self.database = database
        self.port = reserve_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        self.process = subprocess.Popen(
            [
                str(self.binary),
                "server",
                "--bind",
                f"127.0.0.1:{self.port}",
                "--database",
                str(self.database),
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                raise RuntimeError(f"server exited during startup: {stderr}")
            try:
                request(self.base_url, "GET", "/health")
                return
            except (OSError, urllib.error.URLError):
                time.sleep(0.02)
        raise TimeoutError("server did not become ready")

    def stop(self) -> None:
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.process = None

    def __enter__(self) -> Server:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def reserve_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    data = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, dict(response.headers.items()), response.read()


def watch(base_url: str, revision: int, count: int) -> None:
    path = f"/api/v1/tasks?watch=true&revision={revision}"
    req = urllib.request.Request(
        base_url + path,
        headers={"Accept": "text/event-stream"},
        method="GET",
    )
    observed = 0
    with urllib.request.urlopen(req, timeout=10) as response:
        while observed < count:
            line = response.readline()
            if not line:
                raise RuntimeError("watch closed before the expected event count")
            if line.startswith(b"id: "):
                observed += 1


def measure(name: str, samples: int, operation: Callable[[], None]) -> dict[str, Any]:
    operation()
    timings = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        operation()
        timings.append((time.perf_counter_ns() - started) / 1_000_000)
    ordered = sorted(timings)
    p95 = ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]
    return {
        "operation": name,
        "median_ms": statistics.median(timings),
        "p95_ms": p95,
        "max_ms": max(timings),
        "samples": samples,
    }


def initialize_database(binary: Path, database: Path) -> None:
    server = Server(binary, database)
    server.start()
    server.stop()


def seed(database: Path, task_count: int, version_count: int) -> None:
    connection = sqlite3.connect(database)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(INSERT_SQL, rows(task_count, version_count))
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()


def run_benchmark(config: argparse.Namespace, database: Path) -> list[dict[str, Any]]:
    task_id = uuid_v7_text(1)
    operations: list[tuple[str, Callable[[], None]]] = []
    with Server(config.server_binary, database) as server:
        base = server.base_url
        operations.extend(
            [
                ("GET /health", lambda: request(base, "GET", "/health")),
                (
                    "GET /api/v1/tasks/{id}",
                    lambda: request(base, "GET", f"/api/v1/tasks/{task_id}"),
                ),
                ("GET /api/v1/tasks", lambda: request(base, "GET", "/api/v1/tasks")),
                (
                    "GET /api/v1/tasks?list=in",
                    lambda: request(base, "GET", "/api/v1/tasks?list=in"),
                ),
                (
                    "GET /api/v1/tasks?state=pending",
                    lambda: request(base, "GET", "/api/v1/tasks?state=pending"),
                ),
                (
                    "GET /api/v1/tasks?labels=...",
                    lambda: request(
                        base,
                        "GET",
                        "/api/v1/tasks?labels="
                        + urllib.parse.quote("suite:api-performance"),
                    ),
                ),
                (
                    "GET /api/v1/tasks?list+state+labels",
                    lambda: request(
                        base,
                        "GET",
                        "/api/v1/tasks?list=in&state=pending&labels="
                        + urllib.parse.quote("suite:api-performance"),
                    ),
                ),
            ]
        )

        create_counter = 0

        def create() -> None:
            nonlocal create_counter
            create_counter += 1
            request(
                base,
                "POST",
                "/api/v1/tasks",
                {"description": f"measured create {create_counter}"},
            )

        operations.append(("POST /api/v1/tasks", create))

        put_targets: list[TaskTarget] = []
        for number in range(config.samples + 1):
            _, _, payload = request(
                base,
                "POST",
                "/api/v1/tasks",
                {"description": f"PUT target {number}"},
            )
            task = json.loads(payload)
            put_targets.append(
                TaskTarget(task["metadata"]["id"], task["metadata"]["revision"])
            )
        put_index = 0

        def put() -> None:
            nonlocal put_index
            target = put_targets[put_index]
            put_index += 1
            request(
                base,
                "PUT",
                f"/api/v1/tasks/{target.task_id}",
                {
                    "metadata": {"revision": target.revision},
                    "description": "measured update",
                    "list": "in",
                    "state": "doing",
                    "labels": {},
                },
            )

        operations.append(("PUT /api/v1/tasks/{id}", put))

        _, headers, _ = request(base, "GET", "/api/v1/tasks?list=in")
        current_revision = int(
            next(value for key, value in headers.items() if key.lower() == "x-revision")
        )
        operations.extend(
            [
                (
                    "GET watch replay 1 event",
                    lambda: watch(base, current_revision, 1),
                ),
                (
                    "GET watch replay 256 events",
                    lambda: watch(base, current_revision - 255, 256),
                ),
            ]
        )

        return [measure(name, config.samples, operation) for name, operation in operations]


class TaskTarget:
    def __init__(self, task_id: str, revision: int) -> None:
        self.task_id = task_id
        self.revision = revision


def write_results(config: argparse.Namespace, records: list[dict[str, Any]]) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    with (RESULTS / "timings.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    environment = {
        "date": time.strftime("%Y-%m-%d", time.gmtime()),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "tasks": config.tasks,
        "versions_per_task": config.versions,
        "seed_events": config.tasks * config.versions,
        "samples": config.samples,
        "threshold_ms": config.threshold_ms,
        "server_binary": str(config.server_binary.resolve()),
    }
    (RESULTS / "environment.json").write_text(
        json.dumps(environment, indent=2) + "\n", encoding="utf-8"
    )


def main() -> int:
    config = arguments()
    if config.tasks < 1_001 or config.versions < 101:
        raise SystemExit("the acceptance run requires --tasks >= 1001 and --versions >= 101")
    if config.samples < 1:
        raise SystemExit("--samples must be positive")
    if not config.server_binary.is_file():
        raise SystemExit(f"release server binary not found: {config.server_binary}")

    with tempfile.TemporaryDirectory(prefix="gtd-api-performance-") as temporary:
        database = Path(temporary) / "benchmark.db"
        initialize_database(config.server_binary, database)
        seed(database, config.tasks, config.versions)
        records = run_benchmark(config, database)

    write_results(config, records)
    failed = [record for record in records if record["max_ms"] >= config.threshold_ms]
    for record in records:
        print(
            f"{record['operation']:<42} median={record['median_ms']:8.2f} ms "
            f"p95={record['p95_ms']:8.2f} ms max={record['max_ms']:8.2f} ms"
        )
    if failed:
        print("latency threshold failed", file=sys.stderr)
        return 1
    print(f"all API samples are below {config.threshold_ms:.0f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
