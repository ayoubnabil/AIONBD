# syntax=docker/dockerfile:1

FROM rust:bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY core ./core
COPY server ./server
COPY bench ./bench

RUN cargo build --release -p aionbd-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --home /nonexistent --shell /usr/sbin/nologin aionbd

WORKDIR /app
COPY --from=builder /app/target/release/aionbd-server /usr/local/bin/aionbd-server

RUN mkdir -p /var/lib/aionbd && chown -R aionbd:aionbd /var/lib/aionbd

USER aionbd

ENV AIONBD_BIND=0.0.0.0:8080 \
    AIONBD_PERSISTENCE_ENABLED=true \
    AIONBD_SNAPSHOT_PATH=/var/lib/aionbd/snapshot.json \
    AIONBD_WAL_PATH=/var/lib/aionbd/wal.jsonl

VOLUME ["/var/lib/aionbd"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/aionbd-server"]
