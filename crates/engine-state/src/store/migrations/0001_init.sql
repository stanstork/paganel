-- Run state, checkpoints, WAL and receipts.

CREATE TABLE runs (
    run_id     TEXT PRIMARY KEY,
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE checkpoints (
    run_id    TEXT NOT NULL,
    item_id   TEXT NOT NULL,
    part_id   TEXT NOT NULL,
    batch_id  TEXT NOT NULL,
    stage     INTEGER NOT NULL,
    rows_done INTEGER NOT NULL,
    data      TEXT NOT NULL,
    PRIMARY KEY (run_id, item_id, part_id)
);

CREATE TABLE wal (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    entry  TEXT NOT NULL
);
CREATE INDEX wal_run ON wal (run_id, seq);

CREATE TABLE receipts (
    pipeline   TEXT NOT NULL,
    table_name TEXT NOT NULL,
    receipt    TEXT NOT NULL,
    PRIMARY KEY (pipeline, table_name)
);
