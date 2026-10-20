#!/usr/bin/env bash
# Local PostgreSQL for development and tests, without Docker.
# Uses the Homebrew binaries, a data directory inside the repo (.local/pg)
# and its own port so it never collides with another Postgres on the machine.
# WAL is capped at 128 MB: the default (1 GB) filled a nearly full laptop disk
# during the write benchmarks in scripts/explain.py.
#
#   scripts/dev-db.sh up     initdb on first use, then start
#   scripts/dev-db.sh down   stop
#   scripts/dev-db.sh status
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/.local/pg"
PORT="${MV_PG_PORT:-5441}"
PG_BIN="${PG_BIN:-$(dirname "$(command -v pg_ctl || echo /opt/homebrew/opt/postgresql@17/bin/pg_ctl)")}"

case "${1:-up}" in
  up)
    if [ ! -d "$DATA" ]; then
      mkdir -p "$ROOT/.local"
      LC_ALL=C "$PG_BIN/initdb" -D "$DATA" -U postgres --auth=trust -E UTF8 --locale=C >/dev/null
    fi
    if ! "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
      LC_ALL=C "$PG_BIN/pg_ctl" -D "$DATA" -w \
        -o "-p $PORT -k /tmp -c max_connections=200 -c shared_buffers=256MB -c max_wal_size=128MB -c min_wal_size=32MB" \
        -l "$ROOT/.local/pg.log" start >/dev/null
    fi
    for db in macro_vintages macro_vintages_test; do
      "$PG_BIN/psql" -h 127.0.0.1 -p "$PORT" -U postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1 \
        || "$PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres "$db"
    done
    echo "postgres up on 127.0.0.1:$PORT (data: .local/pg)"
    ;;
  down)
    [ -d "$DATA" ] && "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast >/dev/null 2>&1 || true
    echo "postgres stopped"
    ;;
  status)
    "$PG_BIN/pg_ctl" -D "$DATA" status
    ;;
  *)
    echo "usage: $0 up|down|status" >&2
    exit 2
    ;;
esac
