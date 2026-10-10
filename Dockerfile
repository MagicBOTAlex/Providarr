# syntax=docker/dockerfile:1
# External-Postgres image: Providarr only, connects to a Postgres you provide.

# ---- build stage -----------------------------------------------------------
FROM rust:1.99-bookworm@sha256:59037199c44290f2befcdd58dcc540164763fc296950255aaefeef096a1866b0 AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config cmake clang libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --locked

# ---- runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 1000 --home-dir /app --create-home --shell /usr/sbin/nologin providarr

WORKDIR /app
COPY --from=builder /build/target/release/providarr /usr/local/bin/providarr
COPY config ./config
COPY LICENSE ./LICENSE
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN mkdir -p /app/logs && chown -R providarr:providarr /app/logs /app/config

ENV PROVIDARR_CONFIG=/app/config/config.json
EXPOSE 4155
VOLUME ["/app/logs"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:4155/health || exit 1

# Runs as root only long enough to map PUID/PGID/TZ and chown /app, then drops
# to the `providarr` user via gosu.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
