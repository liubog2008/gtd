# SQLite latest-event query benchmark

This directory is a self-contained benchmark comparing two ways to load the
latest event for every task:

1. `GROUP BY task_id` with `MAX(revision)`;
2. a recursive loose index scan over `(task_id, revision DESC)`.

The completed 20-cell run and conclusions are in [report.md](report.md).

The benchmark is deliberately isolated from the application:

- it imports only Python standard-library modules;
- it does not import or execute any code under `src/`;
- it creates one synthetic SQLite database per `N` in an operating-system
  temporary directory;
- it never discovers or opens an application database;
- temporary databases are deleted automatically;
- only timing, environment, and query-plan files are written under `results/`.

Run the complete matrix:

```sh
python3 benchmarks/sqlite-latest-event/benchmark.py
```

Run a small smoke test:

```sh
python3 benchmarks/sqlite-latest-event/benchmark.py \
  --tasks 1,10 \
  --versions 1,10 \
  --samples 3 \
  --target-sample-seconds 0.02 \
  --output-dir /tmp/sqlite-latest-event-smoke
```

The default matrix is:

- `N = 1, 10, 100, 1,000, 10,000` tasks;
- `V = 1, 10, 100, 1,000` versions per task;
- seven warm-cache timing samples per cell;
- adaptive repetitions for fast queries, targeting at least 100 ms per sample.

`queries.sql` contains the two SQL statements in directly readable form. The
constants in `benchmark.py` are the executable copies used by the test.
