# Multi-stage build: the same final image carries both
# `partly-proxy-runner` (the proxy + health server) and
# `partly-proxy-echo` (the deterministic test upstream). The
# compose stack picks an entrypoint per service via `command:`.

FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json

# Add a dependencies build layer
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo chef cook --release --recipe-path recipe.json

# Copy the whole workspace.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release \
    --bin partly-proxy-runner \
    --bin partly-proxy-echo \
    && cp /build/target/release/partly-proxy-runner /usr/local/bin/ \
    && cp /build/target/release/partly-proxy-echo /usr/local/bin/

FROM debian:bookworm-slim AS runtime

# ca-certificates so the proxy can verify HTTPS upstreams against the
# Mozilla root store. Everything else (rustls, ring) is statically linked
# into the binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/partly-proxy-runner /usr/local/bin/
COPY --from=builder /usr/local/bin/partly-proxy-echo /usr/local/bin/

ENV RUST_LOG=info \
    PARTLY_PROXY_BIND=0.0.0.0:8080 \
    PARTLY_PROXY_HEALTH_BIND=0.0.0.0:9090 \
    PARTLY_PROXY_TCP_CONTROL_BIND=0.0.0.0:4500

EXPOSE 8080 9090 4500

ENTRYPOINT ["partly-proxy-runner"]
