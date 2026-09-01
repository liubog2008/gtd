-- Scheme A: scan the eligible event history and aggregate the latest revision
-- for every task. :read_revision = 0 is resolved by the caller before running
-- this statement; the benchmark binds the current MAX(revision).
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

-- Scheme B: explicitly perform a loose index scan over distinct task IDs, then
-- seek the latest eligible revision for each task. AS MATERIALIZED prevents the
-- correlated latest-revision lookup from being evaluated more than once.
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
