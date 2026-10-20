-- Baselines from `pag state import`, one per import name.
-- The receipt itself lives in `receipts` under pipeline `@import/{name}`.

CREATE TABLE imports (
    name       TEXT PRIMARY KEY,
    connection TEXT NOT NULL,  -- connection block name, never the URL
    record     TEXT NOT NULL   -- ImportRecord as JSON
);
