#!/usr/bin/env bash
# LinuxServer.io-style init for the external-Postgres image.
#
# Applies PUID/PGID/TZ, makes the writable mounts owned by the target user, and
# drops privileges numerically with setpriv. This deliberately does NOT rewrite
# /etc/passwd and treats the root filesystem as possibly read-only, so it works
# with `read_only: true` + `cap_drop: [ALL]` (given the CAP_* the compose adds).
set -euo pipefail

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

# Best-effort: on a read-only rootfs /etc cannot be written, but the TZ
# environment variable is still honoured by libc, so this is not fatal.
if [ -f "/usr/share/zoneinfo/$TZ" ]; then
    { ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime; echo "$TZ" > /etc/timezone; } 2>/dev/null || true
else
    echo "[entrypoint] warning: unknown TZ '$TZ'; leaving the timezone unchanged" >&2
fi

# The config/logs directories may be bind-mounted from the host; make them
# writable by the target user. Best-effort so a read-only or absent mount does
# not abort startup.
mkdir -p /app/logs 2>/dev/null || true
chown -R "$PUID:$PGID" /app/logs 2>/dev/null || true
chown -R "$PUID:$PGID" /app/config 2>/dev/null || true

echo "[entrypoint] starting providarr as uid=${PUID} gid=${PGID} TZ=${TZ}"
exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups /usr/local/bin/providarr "$@"
