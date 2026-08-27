PRAGMA foreign_keys = ON;

CREATE TABLE tasks (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    description TEXT NOT NULL CHECK (length(trim(description)) > 0),
    list_name TEXT NOT NULL CHECK (list_name IN ('in', 'next-action', 'waiting-for', 'someday-maybe', 'archive')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'doing', 'done', 'trash')),
    context_note TEXT,
    revisit_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CHECK (
        (list_name = 'archive' AND state IN ('done', 'trash')) OR
        (list_name <> 'archive' AND state IN ('pending', 'doing'))
    )
);

CREATE INDEX tasks_list_state_created_idx
    ON tasks (list_name, state, created_at, id);
CREATE INDEX tasks_revisit_idx
    ON tasks (revisit_at)
    WHERE revisit_at IS NOT NULL;

CREATE TABLE labels (
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    key TEXT NOT NULL CHECK (length(trim(key)) > 0),
    value TEXT NOT NULL CHECK (length(trim(value)) > 0),
    PRIMARY KEY (task_id, key)
);

CREATE INDEX labels_key_value_task_idx ON labels (key, value, task_id);

