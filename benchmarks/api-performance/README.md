# API performance benchmark

This benchmark is isolated from application data. It creates a temporary SQLite database,
inserts synthetic task history, starts a release `gtd server` on loopback, exercises every public
HTTP API category, and deletes the database when it exits.

Build and run:

```bash
cargo build --release
python3 benchmarks/api-performance/benchmark.py
```

Defaults:

- 1,024 tasks;
- 101 versions per task (103,424 seeded events);
- 7 end-to-end HTTP samples per operation;
- 1,000 ms maximum latency threshold for every sample.

The synthetic rows bypass Repository transition validation solely to create deep histories that
the finite GTD state machine cannot naturally produce. The Server, migrations, SQLite queries,
serialization, TCP, and HTTP stack under measurement are the real release implementation.

