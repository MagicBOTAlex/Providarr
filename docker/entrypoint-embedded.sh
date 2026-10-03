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
    gosu postgres createdb -O "${DB_USER}" "${DB_NAME}"
fi

export DATABASE_URL="${DATABASE_URL:-postgres://${DB_USER}:${DB_PASSWORD}@127.0.0.1:${PGPORT}/${DB_NAME}}"
echo "[entrypoint] launching providarr"

exec gosu providarr /usr/local/bin/providarr
