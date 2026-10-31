-- `pag drift` run history, newest KEPT_RUNS per config.

CREATE TABLE drift_runs (
    run_id    TEXT PRIMARY KEY,  -- drift-{UTC timestamp}, sorts by time
    config    TEXT NOT NULL,     -- absolute config path
    exit_code INTEGER NOT NULL,
    run       TEXT NOT NULL      -- DriftRun as JSON
);
CREATE INDEX drift_runs_config ON drift_runs (config, run_id);