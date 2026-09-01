PRAGMA foreign_keys = ON;

CREATE TABLE task_events (
    revision INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    prev_revision INTEGER,
    event_type TEXT NOT NULL CHECK (event_type IN ('task.created', 'task.updated')),

    description TEXT NOT NULL CHECK (length(trim(description)) > 0),
    list_name TEXT NOT NULL CHECK (list_name IN ('in', 'next-action', 'waiting-for', 'someday-maybe', 'archive')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'doing', 'done', 'trash')),
    labels TEXT NOT NULL CHECK (json_valid(labels) AND json_type(labels) = 'object'),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    prev_description TEXT,
    prev_list_name TEXT CHECK (
        prev_list_name IS NULL OR
        prev_list_name IN ('in', 'next-action', 'waiting-for', 'someday-maybe', 'archive')
    ),
    prev_state TEXT CHECK (
        prev_state IS NULL OR prev_state IN ('pending', 'doing', 'done', 'trash')
    ),
    prev_labels TEXT CHECK (
        prev_labels IS NULL OR
        (json_valid(prev_labels) AND json_type(prev_labels) = 'object')
    ),
    prev_updated_at TIMESTAMP,

    CHECK (
        length(task_id) = 36
        AND task_id = lower(task_id)
        AND substr(task_id, 9, 1) = '-'
        AND substr(task_id, 14, 1) = '-'
        AND substr(task_id, 15, 1) = '7'
        AND substr(task_id, 19, 1) = '-'
        AND substr(task_id, 24, 1) = '-'
        AND substr(task_id, 20, 1) IN ('8', '9', 'a', 'b')
    ),
    CHECK (prev_revision IS NULL OR prev_revision < revision),
    CHECK (updated_at >= created_at),
    CHECK (
        (list_name = 'archive' AND state IN ('done', 'trash')) OR
        (list_name <> 'archive' AND state IN ('pending', 'doing'))
    ),
    CHECK (
        (
            prev_revision IS NULL
            AND prev_description IS NULL
            AND prev_list_name IS NULL
            AND prev_state IS NULL
            AND prev_labels IS NULL
            AND prev_updated_at IS NULL
        ) OR (
            prev_revision IS NOT NULL
            AND prev_description IS NOT NULL
            AND prev_list_name IS NOT NULL
            AND prev_state IS NOT NULL
            AND prev_labels IS NOT NULL
            AND prev_updated_at IS NOT NULL
        )
    )
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

CREATE TRIGGER task_events_reject_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task_events is append-only');
END;

CREATE TABLE task_event_compaction (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    scheduled_revision INTEGER NOT NULL DEFAULT 0 CHECK (scheduled_revision >= 0),
    finished_revision INTEGER NOT NULL DEFAULT 0 CHECK (
        finished_revision >= 0
        AND finished_revision <= scheduled_revision
    )
);

INSERT INTO task_event_compaction (
    singleton,
    scheduled_revision,
    finished_revision
) VALUES (1, 0, 0);
