#!/usr/bin/env bash
# Starts an embedded Postgres, ensures the Providarr role/database exist, then
# runs Providarr. As PID 1 this script must forward signals to Providarr and
# shut Postgres down cleanly before exiting.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
DB_USER="${POSTGRES_USER:-providarr}"
DB_PASSWORD="${POSTGRES_PASSWORD:-providarr}"
DB_NAME="${POSTGRES_DB:-providarr}"
PGPORT="${PGPORT:-5432}"

if ! [[ "$PGPORT" =~ ^[0-9]+$ ]] || (( PGPORT < 1 || PGPORT > 65535 )); then
    echo "[entrypoint] invalid PGPORT '$PGPORT': expected an integer in 1-65535" >&2
    exit 1
fi

# LinuxServer.io-style PUID/PGID/TZ mapping for the Providarr process. The
# embedded Postgres keeps its own `postgres` user and its own data ownership.
PUID="${PUID:-1000}"
PGID="${PGID:-1000}"
TZ="${TZ:-Etc/UTC}"

case "$PUID" in
    '' | *[!0-9]*)
        echo "[entrypoint] invalid PUID '$PUID': expected a numeric uid" >&2
        exit 1
        ;;
esac
case "$PGID" in
    '' | *[!0-9]*)
        echo "[entrypoint] invalid PGID '$PGID': expected a numeric gid" >&2
        exit 1
        ;;
esac

if [ -f "/usr/share/zoneinfo/$TZ" ]; then
    { ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime; echo "$TZ" > /etc/timezone; } 2>/dev/null || true
else
    echo "[entrypoint] warning: unknown TZ '$TZ'; leaving the timezone unchanged" >&2
fi

mkdir -p /app/logs /app/config 2>/dev/null || true
chown -R "$PUID:$PGID" /app/logs 2>/dev/null || true
chown -R "$PUID:$PGID" /app/config 2>/dev/null || true

mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[entrypoint] initializing embedded postgres at $PGDATA"
    gosu postgres initdb -D "$PGDATA" \
        --auth-local=trust \
        --auth-host=scram-sha-256 \
        -U postgres >/dev/null
fi

echo "[entrypoint] starting embedded postgres on 127.0.0.1:$PGPORT"
gosu postgres pg_ctl -D "$PGDATA" \
    -o "-c listen_addresses='127.0.0.1' -p $PGPORT" \
    -w start

# Values reach SQL through psql variables (:'var' quotes as a literal, :"var"
# quotes as an identifier), never through string interpolation. The password is
# additionally read from the environment with \getenv so it never appears in
# process argv.
role_exists() {
    gosu postgres psql -X -q -t -A --set=ON_ERROR_STOP=1 -d postgres \
        -v user="$DB_USER" -f - <<'SQL'
SELECT 1 FROM pg_roles WHERE rolname = :'user'
SQL
}

create_role() {
    DB_PASSWORD="$DB_PASSWORD" gosu postgres psql -X -q -t -A \
        --set=ON_ERROR_STOP=1 -d postgres -v user="$DB_USER" -f - <<'SQL'
\getenv pass DB_PASSWORD
CREATE ROLE :"user" LOGIN PASSWORD :'pass' CREATEDB;
SQL
}

database_exists() {
    gosu postgres psql -X -q -t -A --set=ON_ERROR_STOP=1 -d postgres \
        -v name="$DB_NAME" -f - <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'name'
SQL
}

if ! role_exists | grep -q 1; then
    echo "[entrypoint] creating role ${DB_USER}"
    create_role
fi

if ! database_exists | grep -q 1; then
    echo "[entrypoint] creating database ${DB_NAME}"
    gosu postgres createdb -O "$DB_USER" -- "$DB_NAME"
fi

export DATABASE_URL="${DATABASE_URL:-postgres://${DB_USER}:${DB_PASSWORD}@127.0.0.1:${PGPORT}/${DB_NAME}}"

PROVIDARR_PID=""
postgres_stopped=0

stop_postgres() {
    if [ "$postgres_stopped" -eq 0 ]; then
        postgres_stopped=1
        echo "[entrypoint] stopping embedded postgres"
        gosu postgres pg_ctl -D "$PGDATA" -m fast -w stop || true
    fi
}

shutdown() {
    # Only handle the first signal; ignore further TERM/INT while draining.
    trap '' TERM INT
    echo "[entrypoint] received shutdown signal, stopping providarr"
    if [ -n "$PROVIDARR_PID" ] && kill -0 "$PROVIDARR_PID" 2>/dev/null; then
        kill -TERM "$PROVIDARR_PID" 2>/dev/null || true
        # Give providarr up to ~8s to exit gracefully, but never block longer
        # than Docker's default 10s stop grace period.
        for ((i = 0; i < 80; i++)); do
            kill -0 "$PROVIDARR_PID" 2>/dev/null || break
            sleep 0.1
        done
    fi
    stop_postgres
    if [ -n "$PROVIDARR_PID" ] && kill -0 "$PROVIDARR_PID" 2>/dev/null; then
        kill -KILL "$PROVIDARR_PID" 2>/dev/null || true
    fi
    wait "$PROVIDARR_PID" 2>/dev/null || true
}

trap shutdown TERM INT

echo "[entrypoint] launching providarr as uid=${PUID} gid=${PGID} TZ=${TZ}"
setpriv --reuid="$PUID" --regid="$PGID" --clear-groups /usr/local/bin/providarr &
PROVIDARR_PID=$!

set +e
wait "$PROVIDARR_PID"
STATUS=$?
set -e

stop_postgres
exit "$STATUS"
