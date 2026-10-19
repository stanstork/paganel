#!/usr/bin/env bash
# Paganel benchmark harness (MySQL <-> PostgreSQL bulk load).
#
#   ./benchmarks/run.sh              # benchmark Paganel on every workload
#   ./benchmarks/run.sh clean        # tear down bench containers + volumes
#
# This benchmarks PAGANEL. pgloader is an OPTIONAL comparison and only applies
# to PostgreSQL-target workloads (pgloader only migrates *into* PostgreSQL);
# enable it with WITH_PGLOADER=1. By default only Paganel runs.
#
# Workloads
#   sakila                 full Sakila DB, MySQL -> PostgreSQL: schema + data
#   synthetic              one deterministic ~200 B/row table (default 100M rows), MySQL -> PostgreSQL
#   synthetic_heavy        same table, ~20 computed columns (concat/upper/year/arithmetic);
#                          Paganel only (pgloader has no computed-column transforms)
#   synthetic_plugin_rust  same table, one column via a native-Rust WASM transform plugin
#   synthetic_plugin_js    same table, one column via a JavaScript (QuickJS) WASM plugin
#                          (both Paganel only; need native Paganel + the wasm32 target / npx)
#   reverse                the synthetic table, PostgreSQL -> MySQL (Paganel only; RUN_REVERSE)
#
# Paganel scenarios (TOOLS)
#   paganel            single lane (default)
#   paganel-integrity  --integrity: row hashing + Merkle receipts
#   paganel-lanes      4 parallel PK-range lanes; runs only where a
#                      <workload>_lanes.ppl config exists (currently: synthetic).
#                      Lanes parallelize a table only when it has an integer PK.
#
# Paganel: binary or docker
#   If a Paganel binary exists at PAGANEL_BIN (default target/release/pag) it
#   is measured natively; otherwise Paganel is built and run from Dockerfile.paganel.
#
# pgloader comparison (opt-in: WITH_PGLOADER=1)
#   Adds pgloader to the PostgreSQL-target workloads. Native when PGLOADER_BIN is
#   set, else docker v4 (built from Dockerfile.pgloader). For a fair comparison
#   run both tools the same way - both native, or both docker.
#
# Destination / source databases (override to target your own)
#   PG_DEST_DB     PostgreSQL destination for the MySQL -> PG workloads
#   MYSQL_DEST_DB  MySQL destination for the PG -> MySQL reverse workload
#   PG_SRC_DB      PostgreSQL source db seeded for the reverse workload
#
# Knobs (env vars)
#   BENCH_ROWS=100000000   synthetic table size (scale down for a smoke run)
#   RUNS=3                 repetitions per sakila scenario (median reported)
#   SYNTH_RUNS=1           repetitions per synthetic scenario
#   WORKLOADS="sakila synthetic"
#   TOOLS="paganel paganel-integrity"    Paganel scenarios to run
#   WITH_PGLOADER=0        also run pgloader on PG-target workloads (comparison)
#   PAGANEL_BIN=...        Paganel binary; if it is absent, Paganel runs in docker
#   PGLOADER_BIN=...       local pgloader binary; empty -> docker v4 image
#   PGLOADER_IMAGE / PGLOADER_JAR_URL    docker pgloader image / v4 jar url
#   PAGANEL_IMAGE=paganel-bench:local    image built for docker-mode Paganel
#   RUN_REVERSE=1          also run the PG -> MySQL reverse benchmark (paganel only)
#   REV_ROWS=$BENCH_ROWS   row count for the reverse benchmark's PG source
#   REV_RUNS=$SYNTH_RUNS   repetitions for the reverse benchmark
#   PG_DEST_DB / MYSQL_DEST_DB / PG_SRC_DB   destination / source database names
#
# Results land in benchmarks/results/<timestamp>/ (summary.md, summary.tsv, logs).
# Methodology: ../docs/benchmarks.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/compose.yml" -p paganel-bench)

MYSQL_CTR=paganel-bench-mysql
PG_CTR=paganel-bench-postgres
MYSQL_PORT=33307

PG_PORT=54329
# External-DB mode: point the harness at databases on other hosts. When
# EXTERNAL_DB=1 it skips `compose up` and talks to the DBs with networked
# mysql/psql clients (at MYSQL_HOST:MYSQL_PORT / PG_HOST:PG_PORT) instead of
# `docker exec`. Defaults preserve the single-host local behavior.
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
PG_HOST="${PG_HOST:-127.0.0.1}"
EXTERNAL_DB="${EXTERNAL_DB:-0}"

BENCH_ROWS="${BENCH_ROWS:-100000000}"
RUNS="${RUNS:-3}"
SYNTH_RUNS="${SYNTH_RUNS:-1}"
WORKLOADS="${WORKLOADS:-sakila synthetic synthetic_heavy synthetic_plugin_rust synthetic_plugin_js synthetic_filter_rust synthetic_filter_js}"
TOOLS="${TOOLS:-paganel paganel-integrity paganel-lanes paganel-lanes-integrity}"
# pgloader is an opt-in comparison for PostgreSQL-target workloads only.
WITH_PGLOADER="${WITH_PGLOADER:-0}"
PGLOADER_BIN="${PGLOADER_BIN:-}"
# Docker pgloader is v4 (the JVM rewrite), built locally from Dockerfile.pgloader
# since no v4 image is published. Point PGLOADER_IMAGE at a prebuilt image to use
# that instead (it is pulled, not built). PGLOADER_JAR_URL selects the v4 build.
PGLOADER_IMAGE="${PGLOADER_IMAGE:-pgloader-bench:v4}"
PGLOADER_JAR_URL="${PGLOADER_JAR_URL:-https://github.com/dimitri/pgloader/releases/download/v4-dev/pgloader.jar}"
PAGANEL_IMAGE="${PAGANEL_IMAGE:-paganel-bench:local}"
RUN_REVERSE="${RUN_REVERSE:-1}"
REV_ROWS="${REV_ROWS:-$BENCH_ROWS}"
REV_RUNS="${REV_RUNS:-$SYNTH_RUNS}"
PAGANEL_RUN_CTR=paganel-bench-run
# Destination / source databases (overridable to target your own instances).
PG_DEST_DB="${PG_DEST_DB:-bench_dest}"     # PostgreSQL dest for MySQL -> PG workloads
MYSQL_DEST_DB="${MYSQL_DEST_DB:-bench_rev}" # MySQL dest for the PG -> MySQL reverse
PG_SRC_DB="${PG_SRC_DB:-bench_src}"        # PostgreSQL source seeded for the reverse

# WASM transform plugins for the plugin workloads (built into plugins/build/).
PLUGIN_BUILD_DIR="$SCRIPT_DIR/plugins/build"
PLUGIN_RUST_WASM="$PLUGIN_BUILD_DIR/order_net_rust.wasm"
PLUGIN_JS_WASM="$PLUGIN_BUILD_DIR/order_net_js.wasm"
PLUGIN_FILTER_RUST_WASM="$PLUGIN_BUILD_DIR/order_ok_rust.wasm"
PLUGIN_FILTER_JS_WASM="$PLUGIN_BUILD_DIR/order_ok_js.wasm"

SAKILA_TABLES=(actor address category city country customer film film_actor
    film_category inventory language payment rental staff store)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
log() { printf '\033[1m[bench]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[31m[bench] error:\033[0m %s\n' "$*" >&2; exit 1; }

if [[ "$EXTERNAL_DB" == 1 ]]; then
    # Networked clients (need mysql + psql on this host).
    mysql_root() { mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -uroot -pbench --local-infile=1 2>/dev/null; }
    mysql_scalar() { mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -N -uroot -pbench -e "$1" 2>/dev/null; }
    psql_admin() { PGPASSWORD=bench psql -h "$PG_HOST" -p "$PG_PORT" -U bench -d postgres -v ON_ERROR_STOP=1 -qAt -c "$1"; }
    pg_scalar() { PGPASSWORD=bench psql -h "$PG_HOST" -p "$PG_PORT" -U bench -d "$1" -qAt -c "$2"; }
else
    mysql_root() { docker exec -i "$MYSQL_CTR" mysql -uroot -pbench --local-infile=1 2>/dev/null; }
    mysql_scalar() { docker exec "$MYSQL_CTR" mysql -N -uroot -pbench -e "$1" 2>/dev/null; }
    psql_admin() { docker exec "$PG_CTR" psql -U bench -d postgres -v ON_ERROR_STOP=1 -qAt -c "$1"; }
    pg_scalar() { docker exec "$PG_CTR" psql -U bench -d "$1" -qAt -c "$2"; }
fi

# "1:23.45" or "1:02:03" -> seconds (from GNU time -v output)
wall_to_seconds() {
    awk -F: '{ if (NF == 3) print $1*3600 + $2*60 + $3; else print $1*60 + $2 }' <<<"$1"
}

# Parse GNU `time -v` output file -> "wall_seconds max_rss_mb"
parse_time_v() {
    local wall rss
    wall=$(grep 'Elapsed (wall clock)' "$1" | awk '{print $NF}')
    rss=$(grep 'Maximum resident set size' "$1" | awk '{print $NF}')
    echo "$(wall_to_seconds "$wall") $(awk "BEGIN{printf \"%.1f\", $rss/1024}")"
}

# ---------------------------------------------------------------------------
# clean subcommand
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "clean" ]]; then
    docker rm -f paganel-bench-pgloader "$PAGANEL_RUN_CTR" >/dev/null 2>&1 || true
    docker image rm -f "$PAGANEL_IMAGE" "$PGLOADER_IMAGE" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" down -v
    log "bench containers, volumes, and the paganel image removed"
    exit 0
fi

# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------
command -v docker >/dev/null || die "docker is required"
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
[[ -x /usr/bin/time ]] || die "GNU time (/usr/bin/time) is required"

# Paganel mode: use a binary if one exists at PAGANEL_BIN (default
# target/release/pag), otherwise build and run it from Dockerfile.paganel.
PAGANEL_BIN="${PAGANEL_BIN:-$REPO_ROOT/target/release/pag}"
if [[ -x "$PAGANEL_BIN" ]]; then
    PAGANEL_MODE=native
    PAGANEL_VERSION="$("$PAGANEL_BIN" --version 2>/dev/null | head -1 || echo unknown)"
    log "paganel: native $PAGANEL_BIN ($PAGANEL_VERSION)"
else
    PAGANEL_MODE=docker
    log "paganel: no binary at $PAGANEL_BIN, building docker image $PAGANEL_IMAGE..."
    docker build -f "$SCRIPT_DIR/Dockerfile.paganel" -t "$PAGANEL_IMAGE" "$REPO_ROOT" >&2 \
        || die "failed to build paganel image $PAGANEL_IMAGE"
    PAGANEL_VERSION="$(docker run --rm "$PAGANEL_IMAGE" pag --version 2>/dev/null | head -1 || echo unknown)"
    log "paganel: docker $PAGANEL_IMAGE ($PAGANEL_VERSION)"
fi

# Plugin workloads need native Paganel plus the host toolchain to build the two
# transform plugins (Rust -> wasm32-wasip1, and `pag plugin compile` for JS).
# On any shortfall, drop the plugin workloads with a note rather than aborting.
if [[ " $WORKLOADS " == *" synthetic_plugin_"* || " $WORKLOADS " == *" synthetic_filter_"* ]]; then
    drop_plugins() {
        WORKLOADS="$(tr ' ' '\n' <<<"$WORKLOADS" | grep -vE '^synthetic_(plugin|filter)_' | tr '\n' ' ')"
    }
    if [[ $PAGANEL_MODE != native ]]; then
        log "note: plugin workloads need native Paganel (build $PAGANEL_BIN); skipping synthetic_plugin_*"
        drop_plugins
    else
        log "building transform plugins (Rust -> wasm32, JS -> wasm)..."
        mkdir -p "$PLUGIN_BUILD_DIR"
        if (cd "$REPO_ROOT" && cargo build --manifest-path benchmarks/plugins/rust/order_net/Cargo.toml \
                --target wasm32-wasip1 --release) >&2 \
            && cp "$SCRIPT_DIR/plugins/rust/order_net/target/wasm32-wasip1/release/order_net.wasm" \
                "$PLUGIN_RUST_WASM" \
            && "$PAGANEL_BIN" plugin compile "$SCRIPT_DIR/plugins/js/order_net.js" \
                -o "$PLUGIN_JS_WASM" >&2 \
            && (cd "$REPO_ROOT" && cargo build --manifest-path benchmarks/plugins/rust/order_ok/Cargo.toml \
                --target wasm32-wasip1 --release) >&2 \
            && cp "$SCRIPT_DIR/plugins/rust/order_ok/target/wasm32-wasip1/release/order_ok.wasm" \
                "$PLUGIN_FILTER_RUST_WASM" \
            && "$PAGANEL_BIN" plugin compile "$SCRIPT_DIR/plugins/js/order_ok.js" \
                -o "$PLUGIN_FILTER_JS_WASM" >&2; then
            log "plugins ready: transform ($(basename "$PLUGIN_RUST_WASM"), $(basename "$PLUGIN_JS_WASM")), filter ($(basename "$PLUGIN_FILTER_RUST_WASM"), $(basename "$PLUGIN_FILTER_JS_WASM"))"
        else
            log "note: plugin build failed (needs the wasm32-wasip1 target + npx); skipping plugin/filter workloads"
            drop_plugins
        fi
    fi
fi

# pgloader is an opt-in comparison (WITH_PGLOADER=1), for PG-target workloads
# only. Native via PGLOADER_BIN, else docker v4. Run both tools the same way
# (both native or both docker) for a fair comparison - the harness won't force it.
PGLOADER_MODE=skip
PGLOADER_CMD=()   # how to invoke native pgloader (a binary, or `java -jar` a .jar)
if [[ "$WITH_PGLOADER" == 1 ]]; then
    if [[ -n "$PGLOADER_BIN" ]]; then
        if [[ "$PGLOADER_BIN" == *.jar ]]; then
            # pgloader v4 ships as a JAR - run it with Java (UTC, or Connector/J
            # throws HOUR_OF_DAY on DST-gap timestamps).
            [[ -f "$PGLOADER_BIN" ]] || die "PGLOADER_BIN='$PGLOADER_BIN' not found"
            command -v java >/dev/null \
                || die "java (21+) is required to run the pgloader jar; unset PGLOADER_BIN for the docker image"
            PGLOADER_CMD=(java -Duser.timezone=UTC -jar "$PGLOADER_BIN")
        elif [[ -x "$PGLOADER_BIN" ]] || command -v "$PGLOADER_BIN" >/dev/null; then
            PGLOADER_CMD=("$PGLOADER_BIN")
        else
            die "PGLOADER_BIN='$PGLOADER_BIN' is not an executable pgloader or a .jar"
        fi
        PGLOADER_MODE=native
        log "pgloader: native ${PGLOADER_CMD[*]} ($("${PGLOADER_CMD[@]}" --version 2>/dev/null | head -1))"
    else
        PGLOADER_MODE=docker
        if [[ "$PGLOADER_IMAGE" == pgloader-bench:v4 ]]; then
            # Build pgloader v4 (JVM rewrite) from its JAR - no v4 image exists.
            log "pgloader: PGLOADER_BIN unset, building v4 image $PGLOADER_IMAGE from $PGLOADER_JAR_URL"
            docker build -f "$SCRIPT_DIR/Dockerfile.pgloader" \
                --build-arg "PGLOADER_JAR_URL=$PGLOADER_JAR_URL" \
                -t "$PGLOADER_IMAGE" "$SCRIPT_DIR/pgloader" >&2 \
                || die "failed to build pgloader v4 image $PGLOADER_IMAGE"
        else
            # Explicit override: use the given prebuilt image as-is.
            log "pgloader: PGLOADER_BIN unset, using docker image $PGLOADER_IMAGE (default settings)"
            docker image inspect "$PGLOADER_IMAGE" >/dev/null 2>&1 || docker pull "$PGLOADER_IMAGE"
        fi
    fi
    if [[ $PAGANEL_MODE != "$PGLOADER_MODE" ]]; then
        log "note: paganel is $PAGANEL_MODE but pgloader is $PGLOADER_MODE - for a fair"
        log "      comparison run both the same way (set PAGANEL_BIN + PGLOADER_BIN, or neither)."
    fi
fi

# ---------------------------------------------------------------------------
# databases up + seed
# ---------------------------------------------------------------------------
if [[ "$EXTERNAL_DB" == 1 ]]; then
    log "external databases: mysql@$MYSQL_HOST:$MYSQL_PORT, postgres@$PG_HOST:$PG_PORT (compose skipped)"
else
    log "starting bench databases (mysql:$MYSQL_PORT, postgres:$PG_PORT)..."
    "${COMPOSE[@]}" up -d --wait
fi

# The compose user only gets grants on `sakila`; give it the synthetic db too.
mysql_scalar "CREATE DATABASE IF NOT EXISTS bench;
              GRANT ALL PRIVILEGES ON bench.* TO 'bench'@'%';
              ALTER USER 'bench'@'%' IDENTIFIED WITH mysql_native_password BY 'bench';
              FLUSH PRIVILEGES;" >/dev/null

# Generate (or reuse) the synthetic table. Deterministic, so an existing table
# with the right row count is byte-identical to a fresh one.
current_rows=$(mysql_scalar "SELECT COUNT(*) FROM bench.orders" || echo 0)
if [[ "$current_rows" != "$BENCH_ROWS" ]]; then
    log "generating bench.orders with $BENCH_ROWS rows (have: $current_rows). This is one-time and cached."
    mysql_root <"$SCRIPT_DIR/synthetic/generate_mysql.sql"
    gen_start=$(date +%s)
    echo "CALL bench.gen_orders($BENCH_ROWS);" | mysql_root
    log "generation took $(( $(date +%s) - gen_start ))s"
    [[ "$(mysql_scalar 'SELECT COUNT(*) FROM bench.orders')" == "$BENCH_ROWS" ]] \
        || die "generation produced wrong row count"
else
    log "reusing existing bench.orders ($current_rows rows)"
fi

sakila_rows=0
for t in "${SAKILA_TABLES[@]}"; do
    sakila_rows=$(( sakila_rows + $(mysql_scalar "SELECT COUNT(*) FROM sakila.$t") ))
done
log "source row counts: sakila=$sakila_rows synthetic=$BENCH_ROWS"

# ---------------------------------------------------------------------------
# results dir + environment capture
# ---------------------------------------------------------------------------
RESULTS="$SCRIPT_DIR/results/$(date -u +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS"
{
    echo "date_utc: $(date -u +%FT%TZ)"
    echo "commit: $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "paganel: $PAGANEL_VERSION ($PAGANEL_MODE)"
    case $PGLOADER_MODE in
        native) echo "pgloader: $("${PGLOADER_CMD[@]}" --version 2>/dev/null | head -1) (native: ${PGLOADER_CMD[*]})" ;;
        docker)
            if [[ "$PGLOADER_IMAGE" == pgloader-bench:v4 ]]; then
                echo "pgloader: v4 $PGLOADER_IMAGE (docker, jar=$PGLOADER_JAR_URL)"
            else
                echo "pgloader: $PGLOADER_IMAGE (docker)"
            fi ;;
        *) echo "pgloader: off (WITH_PGLOADER=1 to compare)" ;;
    esac
    echo "databases: pg_dest=$PG_DEST_DB mysql_dest=$MYSQL_DEST_DB pg_src=$PG_SRC_DB"
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "cores: $(nproc)"
    echo "mem_total: $(awk '/MemTotal/{printf "%.1f GB", $2/1048576}' /proc/meminfo)"
    echo "kernel: $(uname -sr)"
    echo "docker: $(docker --version)"
    echo "bench_rows: $BENCH_ROWS"
    echo "runs: sakila=$RUNS synthetic=$SYNTH_RUNS"
    if [[ "$RUN_REVERSE" == 1 ]]; then
        echo "reverse: pg->mysql rows=$REV_ROWS runs=$REV_RUNS (paganel only)"
    fi
} | tee "$RESULTS/env.txt" >&2

TSV="$RESULTS/summary.tsv"
echo -e "workload\ttool\trun\twall_s\trows\trows_per_s\tpeak_rss_mb\tstate_mb" >"$TSV"

# ---------------------------------------------------------------------------
# per-run plumbing
# ---------------------------------------------------------------------------
reset_dest_db() { # $1 = pg database name
    psql_admin "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null
    psql_admin "CREATE DATABASE $1 OWNER bench" >/dev/null
}

validate() { # $1 = workload, $2 = pg db
    local bad=0
    if [[ "$1" == sakila ]]; then
        for t in "${SAKILA_TABLES[@]}"; do
            local src dst
            src=$(mysql_scalar "SELECT COUNT(*) FROM sakila.$t")
            dst=$(pg_scalar "$2" "SELECT COUNT(*) FROM $t" || echo MISSING)
            [[ "$src" == "$dst" ]] || { log "  MISMATCH $t: src=$src dst=$dst"; bad=1; }
        done
    else
        # synthetic writes `orders`; synthetic_heavy projects into `orders_heavy`.
        local tbl=orders
        case "$1" in
            synthetic_heavy) tbl=orders_heavy ;;
            synthetic_plugin_rust) tbl=orders_plugin_rust ;;
            synthetic_plugin_js) tbl=orders_plugin_js ;;
            synthetic_filter_rust) tbl=orders_filter_rust ;;
            synthetic_filter_js) tbl=orders_filter_js ;;
        esac
        local dst
        dst=$(pg_scalar "$2" "SELECT COUNT(*) FROM $tbl" || echo MISSING)
        [[ "$dst" == "$BENCH_ROWS" ]] || { log "  MISMATCH $tbl: src=$BENCH_ROWS dst=$dst"; bad=1; }
    fi
    return $bad
}

# Reverse benchmark (PG -> MySQL) plumbing.
reset_mysql_db() { # $1 = mysql database name
    mysql_scalar "DROP DATABASE IF EXISTS $1;
                  CREATE DATABASE $1;
                  GRANT ALL PRIVILEGES ON $1.* TO 'bench'@'%';
                  FLUSH PRIVILEGES;" >/dev/null
}

validate_reverse() { # $1 = mysql dest db, $2 = expected rows
    local dst
    dst=$(mysql_scalar "SELECT COUNT(*) FROM $1.orders" || echo MISSING)
    [[ "$dst" == "$2" ]] || { log "  MISMATCH orders: src=$2 dst=$dst"; return 1; }
    return 0
}

# Poll docker stats for a container's memory while it runs; track the peak.
# The running max is written to the outfile on every sample so the parent can
# kill this loop at any point without losing the measurement.
# Sampled (~2s resolution) - approximate, and noted as such in the report.
sample_docker_mem() { # $1 = container name, $2 = outfile
    local max=0 cur
    sleep 1 # let the container come up before the first poll
    while [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == true ]]; do
        cur=$(docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null) || break
        [[ -n "$cur" ]] || break
        cur=$(awk '{v=$1; if (v ~ /GiB/) m=1024; else if (v ~ /MiB/) m=1; else m=1/1024;
                    gsub(/[A-Za-z]/,"",v); printf "%.1f", v*m}' <<<"${cur%% /*}")
        max=$(awk "BEGIN{print ($cur > $max) ? $cur : $max}")
        echo "$max" >"$2"
        sleep 1
    done
}

# The connection URLs every paganel config may reference. $1 = destination db
# name (a PG db for forward workloads; ignored by the reverse config, which
# targets the fixed `bench_rev` MySQL db). Kept in one place so native and
# docker runs pass identical values.
paganel_url_env() { # $1 = forward pg dest db
    PAGANEL_ENV=(
        "BENCH_SAKILA_MYSQL_URL=mysql://bench:bench@$MYSQL_HOST:$MYSQL_PORT/sakila"
        "BENCH_SAKILA_PG_URL=postgres://bench:bench@$PG_HOST:$PG_PORT/$1"
        "BENCH_SYNTH_MYSQL_URL=mysql://bench:bench@$MYSQL_HOST:$MYSQL_PORT/bench"
        "BENCH_SYNTH_PG_URL=postgres://bench:bench@$PG_HOST:$PG_PORT/$1"
        "BENCH_REV_PG_URL=postgres://bench:bench@$PG_HOST:$PG_PORT/$PG_SRC_DB"
        "BENCH_REV_MYSQL_URL=mysql://bench:bench@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DEST_DB"
        "BENCH_PLUGIN_RUST_WASM=$PLUGIN_RUST_WASM"
        "BENCH_PLUGIN_JS_WASM=$PLUGIN_JS_WASM"
        "BENCH_PLUGIN_FILTER_RUST_WASM=$PLUGIN_FILTER_RUST_WASM"
        "BENCH_PLUGIN_FILTER_JS_WASM=$PLUGIN_FILTER_JS_WASM"
    )
}

run_paganel() { # $1 workload, $2 config, $3 dest db, $4 extra flags, $5 log prefix
    local flags=()
    [[ -n "$4" ]] && flags=($4)
    paganel_url_env "$3"

    if [[ $PAGANEL_MODE == docker ]]; then
        # Mirror the dockerized-pgloader invocation: same --network host, same
        # host databases. Wall clock + sampled docker-stats memory, exactly as
        # pgloader is measured in docker mode (so the two are compared like for
        # like). GNU `time -v` can't see into the container, hence this path.
        local envflags=() e
        for e in "${PAGANEL_ENV[@]}"; do envflags+=(-e "$e"); done
        local memfile="$RESULTS/$5.mem" start end rc
        docker rm -f "$PAGANEL_RUN_CTR" >/dev/null 2>&1 || true
        start=$(date +%s.%N)
        docker run --name "$PAGANEL_RUN_CTR" --rm --network host \
            "${envflags[@]}" \
            -v "$SCRIPT_DIR/paganel:/cfg:ro,z" \
            "$PAGANEL_IMAGE" \
            pag apply -c "/cfg/$(basename "$2")" "${flags[@]}" \
            >"$RESULTS/$5.log" 2>&1 &
        local waiter=$!
        sample_docker_mem "$PAGANEL_RUN_CTR" "$memfile" &
        local sampler=$!
        rc=0
        wait "$waiter" || rc=$?
        end=$(date +%s.%N)
        docker rm -f "$PAGANEL_RUN_CTR" >/dev/null 2>&1 || true
        kill "$sampler" 2>/dev/null || true
        wait "$sampler" 2>/dev/null || true
        [[ $rc -eq 0 ]] || die "paganel run failed - see $RESULTS/$5.log"
        local mem
        mem=$(cat "$memfile" 2>/dev/null || echo 0)
        [[ "$mem" == 0 ]] && mem="n/a"
        echo "$(awk "BEGIN{printf \"%.2f\", $end - $start}") $mem"
        return
    fi

    # Native: isolate paganel's state store under a throwaway HOME per run.
    local run_home="$RESULTS/$5.home"
    mkdir -p "$run_home"
    env HOME="$run_home" "${PAGANEL_ENV[@]}" \
        /usr/bin/time -v -o "$RESULTS/$5.time" \
        "$PAGANEL_BIN" apply -c "$2" "${flags[@]}" >"$RESULTS/$5.log" 2>&1 \
        || die "paganel run failed - see $RESULTS/$5.log"
    # On-disk state this run wrote: under --integrity the row-hash log + receipts
    # dominate (~51 B/row for an integer PK); otherwise it's just checkpoints.
    # Measured before cleanup and returned as a third field (MB).
    local state_mb
    state_mb=$(du -sb "$run_home/.paganel" 2>/dev/null | awk '{printf "%.1f", $1/1048576}')
    [[ -z "$state_mb" ]] && state_mb="0.0"
    [[ "${KEEP_STATE:-0}" == 1 ]] || rm -rf "$run_home"
    echo "$(parse_time_v "$RESULTS/$5.time") $state_mb"
}

run_pgloader() { # $1 load template, $2 pg db, $3 log prefix
    local loadfile="$RESULTS/$3.load"
    sed -e "s|@@SAKILA_MYSQL_URL@@|mysql://bench:bench@$MYSQL_HOST:$MYSQL_PORT/sakila|" \
        -e "s|@@SAKILA_PG_URL@@|postgresql://bench:bench@$PG_HOST:$PG_PORT/$2|" \
        -e "s|@@SYNTH_MYSQL_URL@@|mysql://bench:bench@$MYSQL_HOST:$MYSQL_PORT/bench|" \
        -e "s|@@SYNTH_PG_URL@@|postgresql://bench:bench@$PG_HOST:$PG_PORT/$2|" \
        "$1" >"$loadfile"

    if [[ $PGLOADER_MODE == native ]]; then
        /usr/bin/time -v -o "$RESULTS/$3.time" \
            "${PGLOADER_CMD[@]}" "$loadfile" >"$RESULTS/$3.log" 2>&1 \
            || die "pgloader run failed - see $RESULTS/$3.log"
        parse_time_v "$RESULTS/$3.time"
    else
        local ctr=paganel-bench-pgloader memfile="$RESULTS/$3.mem" start end rc
        docker rm -f "$ctr" >/dev/null 2>&1 || true
        start=$(date +%s.%N)
        docker run --name "$ctr" --network host \
            -v "$RESULTS:/bench:ro,z" "$PGLOADER_IMAGE" \
            pgloader "/bench/$3.load" >"$RESULTS/$3.log" 2>&1 &
        local waiter=$!
        sample_docker_mem "$ctr" "$memfile" &
        local sampler=$!
        rc=0
        wait "$waiter" || rc=$?
        end=$(date +%s.%N)
        # Remove the container first: docker stats keeps answering for an
        # exited-but-present container, which would keep the sampler alive.
        docker rm -f "$ctr" >/dev/null 2>&1 || true
        kill "$sampler" 2>/dev/null || true
        wait "$sampler" 2>/dev/null || true
        [[ $rc -eq 0 ]] || die "pgloader run failed - see $RESULTS/$3.log"
        local mem
        mem=$(cat "$memfile" 2>/dev/null || echo 0)
        # A run shorter than the ~2s docker-stats sampling interval yields no
        # samples; report n/a rather than a misleading zero.
        [[ "$mem" == 0 ]] && mem="n/a"
        echo "$(awk "BEGIN{printf \"%.2f\", $end - $start}") $mem"
    fi
}

# ---------------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------------
for workload in $WORKLOADS; do
    case $workload in
        sakila) rows=$sakila_rows; dest_db=$PG_DEST_DB; n_runs=$RUNS ;;
        synthetic) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        synthetic_heavy) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        synthetic_plugin_rust) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        synthetic_plugin_js) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        synthetic_filter_rust) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        synthetic_filter_js) rows=$BENCH_ROWS; dest_db=$PG_DEST_DB; n_runs=$SYNTH_RUNS ;;
        *) die "unknown workload '$workload'" ;;
    esac

    # pgloader is added (when enabled) only to workloads that have a load
    # template. `synthetic_heavy` has none - pgloader has no expression/computed-
    # column transforms, so it can't express that workload - so it's paganel-only.
    workload_tools="$TOOLS"
    [[ $PGLOADER_MODE != skip && -f "$SCRIPT_DIR/pgloader/$workload.load.tpl" ]] \
        && workload_tools="$workload_tools pgloader"

    for tool in $workload_tools; do
        # The lanes scenario needs a `<workload>_lanes.ppl` (integer-PK single
        # table); skip it for workloads without one (e.g. sakila).
        if [[ "$tool" == paganel-lanes* && ! -f "$SCRIPT_DIR/paganel/${workload}_lanes.ppl" ]]; then
            log "[$workload-$tool] no lanes config, skipping"
            continue
        fi

        for run in $(seq 1 "$n_runs"); do
            prefix="$workload-$tool-$run"
            log "[$prefix] resetting destination..."
            reset_dest_db "$dest_db"

            log "[$prefix] running..."
            case $tool in
                paganel)
                    out=$(run_paganel "$workload" "$SCRIPT_DIR/paganel/$workload.ppl" "$dest_db" "" "$prefix") ;;
                paganel-integrity)
                    out=$(run_paganel "$workload" "$SCRIPT_DIR/paganel/$workload.ppl" "$dest_db" "--integrity" "$prefix") ;;
                paganel-lanes)
                    out=$(run_paganel "$workload" "$SCRIPT_DIR/paganel/${workload}_lanes.ppl" "$dest_db" "" "$prefix") ;;
                paganel-lanes-integrity)
                    out=$(run_paganel "$workload" "$SCRIPT_DIR/paganel/${workload}_lanes.ppl" "$dest_db" "--integrity" "$prefix") ;;
                pgloader)
                    out=$(run_pgloader "$SCRIPT_DIR/pgloader/$workload.load.tpl" "$dest_db" "$prefix") ;;
                *) die "unknown tool '$tool'" ;;
            esac

            wall=$(cut -d' ' -f1 <<<"$out")
            rss=$(cut -d' ' -f2 <<<"$out")
            state_mb=$(cut -d' ' -f3 <<<"$out"); [[ -z "$state_mb" ]] && state_mb="-"
            rps=$(awk "BEGIN{printf \"%.0f\", ($wall > 0) ? $rows / $wall : 0}")

            validate "$workload" "$dest_db" || die "[$prefix] row-count validation failed"
            log "[$prefix] wall=${wall}s rows/s=$rps peak_rss=${rss}MB state=${state_mb}MB - counts verified"
            echo -e "$workload\t$tool\t$run\t$wall\t$rows\t$rps\t$rss\t$state_mb" >>"$TSV"
        done
    done
done

# ---------------------------------------------------------------------------
# reverse benchmark: PG -> MySQL (paganel only; separate from the pgloader
# comparison above - pgloader migrates into PostgreSQL, so there is no MySQL
# destination to compare it against). Same harness (reset -> run -> validate),
# a paganel-only tool set, and its own rows in the TSV (workload=reverse).
# ---------------------------------------------------------------------------
if [[ "$RUN_REVERSE" == 1 ]]; then
    log "reverse: seeding PG source $PG_SRC_DB.orders ($REV_ROWS rows)..."
    psql_admin "SELECT 1 FROM pg_database WHERE datname='$PG_SRC_DB'" | grep -q 1 \
        || psql_admin "CREATE DATABASE $PG_SRC_DB OWNER bench" >/dev/null

    rev_have=$(pg_scalar "$PG_SRC_DB" "SELECT COUNT(*) FROM orders" 2>/dev/null || echo 0)
    if [[ "$rev_have" != "$REV_ROWS" ]]; then
        log "reverse: generating $PG_SRC_DB.orders (have: $rev_have). One-time and cached."
        if [[ "$EXTERNAL_DB" == 1 ]]; then
            PGPASSWORD=bench psql -h "$PG_HOST" -p "$PG_PORT" -U bench -d "$PG_SRC_DB" \
                -v ON_ERROR_STOP=1 -v rows="$REV_ROWS" -q -f - \
                <"$SCRIPT_DIR/synthetic/generate_pg.sql" >/dev/null \
                || die "reverse: PG source generation failed"
        else
            docker exec -i "$PG_CTR" psql -U bench -d "$PG_SRC_DB" \
                -v ON_ERROR_STOP=1 -v rows="$REV_ROWS" -q -f - \
                <"$SCRIPT_DIR/synthetic/generate_pg.sql" >/dev/null \
                || die "reverse: PG source generation failed"
        fi
        [[ "$(pg_scalar "$PG_SRC_DB" 'SELECT COUNT(*) FROM orders')" == "$REV_ROWS" ]] \
            || die "reverse: PG source produced wrong row count"
    else
        log "reverse: reusing existing $PG_SRC_DB.orders ($rev_have rows)"
    fi

    for run in $(seq 1 "$REV_RUNS"); do
        prefix="reverse-paganel-$run"
        log "[$prefix] resetting MySQL destination $MYSQL_DEST_DB..."
        reset_mysql_db "$MYSQL_DEST_DB"

        log "[$prefix] running..."
        out=$(run_paganel "reverse" "$SCRIPT_DIR/paganel/synthetic_reverse.ppl" "" "" "$prefix")

        wall=$(cut -d' ' -f1 <<<"$out")
        rss=$(cut -d' ' -f2 <<<"$out")
        state_mb=$(cut -d' ' -f3 <<<"$out"); [[ -z "$state_mb" ]] && state_mb="-"
        rps=$(awk "BEGIN{printf \"%.0f\", ($wall > 0) ? $REV_ROWS / $wall : 0}")

        validate_reverse "$MYSQL_DEST_DB" "$REV_ROWS" || die "[$prefix] row-count validation failed"
        log "[$prefix] wall=${wall}s rows/s=$rps peak_rss=${rss}MB state=${state_mb}MB - counts verified"
        echo -e "reverse\tpaganel\t$run\t$wall\t$REV_ROWS\t$rps\t$rss\t$state_mb" >>"$TSV"
    done
fi

# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
{
    echo "# Paganel benchmark results"
    echo
    if [[ $PGLOADER_MODE == skip ]]; then
        echo "Paganel on MySQL <-> PostgreSQL bulk load. Run with \`WITH_PGLOADER=1\`"
        echo "to add pgloader as a comparison on the PostgreSQL-target workloads."
    else
        echo "Paganel on MySQL <-> PostgreSQL bulk load, with pgloader as a comparison"
        echo "on the PostgreSQL-target workloads (\`reverse\` is paganel-only)."
    fi
    echo
    echo '```'
    cat "$RESULTS/env.txt"
    echo '```'
    echo
    echo "## Medians per scenario"
    echo
    echo "| workload | tool | runs | wall (s, median) | rows | rows/s (median) | peak RSS (MB, max) |"
    echo "|---|---|---|---|---|---|---|"
    awk -F'\t' 'NR>1 { key=$1 FS $2; n[key]++; rows[key]=$5;
                       walls[key]=walls[key] FS $4; rpss[key]=rpss[key] FS $6;
                       if ($7 == "n/a") rss[key]="n/a";
                       else if (rss[key] != "n/a" && $7+0 > rss[key]+0) rss[key]=$7 }
        function median(s,  a, m, i) { m=split(substr(s,2), a, FS);
            for (i=1;i<m;i++) for (j=i+1;j<=m;j++) if (a[j]+0<a[i]+0) {t=a[i];a[i]=a[j];a[j]=t}
            return (m%2) ? a[(m+1)/2] : (a[m/2]+a[m/2+1])/2 }
        END { for (k in n) { split(k, p, FS);
            printf "| %s | %s | %d | %.1f | %s | %.0f | %s |\n",
                p[1], p[2], n[k], median(walls[k]), rows[k], median(rpss[k]), rss[k] } }' \
        "$TSV" | sort
    echo
    echo "## All runs"
    echo
    echo "| workload | tool | run | wall (s) | rows | rows/s | peak RSS (MB) | state on disk (MB) |"
    echo "|---|---|---|---|---|---|---|---|"
    awk -F'\t' 'NR>1 { printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7,$8 }' "$TSV"
    echo
    echo "The \`reverse\` workload is paganel-only: PostgreSQL -> MySQL (\`LOAD DATA\`"
    echo "fast path). pgloader migrates *into* PostgreSQL, so it has no comparison"
    echo "point for a MySQL destination - it is reported apart from the forward rows."
    if [[ $PGLOADER_MODE != skip ]]; then
        echo
        echo "pgloader comparison notes: on \`sakila\`, pgloader v4 also builds secondary"
        echo "indexes and foreign keys (its WITH toggles to skip them were removed), while"
        echo "paganel builds tables + primary keys and secondary indexes but not foreign keys - so \`sakila\` is not scope-matched;"
        echo "\`synthetic\` (table + PK, one table) is the clean head-to-head. Run paganel and"
        echo "pgloader the same way (both native, or both docker) for a fair wall-clock; peak"
        echo "RSS for a dockerized tool is \`docker stats\`-sampled (~1-2s) and approximate."
        echo
        echo "pgloader here runs with DEFAULT tuning (no workers/concurrency/batch/prefetch"
        echo "options) - these are its out-of-the-box numbers, not its ceiling; a tuned"
        echo "pgloader would likely do better."
    fi
} >"$RESULTS/summary.md"

log "done. results in $RESULTS"
echo
cat "$RESULTS/summary.md"
