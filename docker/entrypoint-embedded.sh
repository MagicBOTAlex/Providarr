#!/usr/bin/env bash
# Starts an embedded Postgres, ensures the Providarr role/database exist, then
# runs Providarr in the foreground.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
DB_USER="${POSTGRES_USER:-providarr}"
DB_PASSWORD="${POSTGRES_PASSWORD:-providarr}"
DB_NAME="${POSTGRES_DB:-providarr}"
PGPORT="${PGPORT:-5432}"

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

if ! gosu postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
    echo "[entrypoint] creating role ${DB_USER}"
    gosu postgres psql -c "CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}' CREATEDB;"
fi

if ! gosu postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
    echo "[entrypoint] creating database ${DB_NAME}"
    gosu postgres createdb -O "${DB_USER}" "${DB_NAME}"
fi

export DATABASE_URL="${DATABASE_URL:-postgres://${DB_USER}:${DB_PASSWORD}@127.0.0.1:${PGPORT}/${DB_NAME}}"
echo "[entrypoint] launching providarr"

exec gosu providarr /usr/local/bin/providarr
