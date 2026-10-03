# syntax=docker/dockerfile:1
# External-Postgres image: Providarr only, connects to a Postgres you provide.

# ---- build stage -----------------------------------------------------------
FROM rust:1.99-bookworm AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config cmake clang libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release --locked

# ---- runtime stage ---------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --home-dir /app --create-home --shell /usr/sbin/nologin providarr

WORKDIR /app
COPY --from=builder /build/target/release/providarr /usr/local/bin/providarr
COPY config ./config
COPY LICENSE ./LICENSE

RUN mkdir -p /app/logs && chown -R providarr:providarr /app/logs

USER providarr
ENV PROVIDARR_CONFIG=/app/config/config.json
EXPOSE 4155
VOLUME ["/app/logs"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:4155/health || exit 1

ENTRYPOINT ["providarr"]
