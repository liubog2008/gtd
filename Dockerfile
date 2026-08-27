# syntax=docker/dockerfile:1.7

ARG RUST_VERSION

FROM rust:${RUST_VERSION}-alpine AS builder
ARG RUST_VERSION
WORKDIR /workspace

COPY rust-toolchain.toml /tmp/rust-toolchain.toml

RUN grep -qFx "channel = \"${RUST_VERSION}\"" /tmp/rust-toolchain.toml \
    || { echo "RUST_VERSION=${RUST_VERSION} does not match rust-toolchain.toml" >&2; exit 1; }

RUN apk add --no-cache build-base

COPY Cargo.toml Cargo.lock ./

RUN cargo fetch \
    --locked

COPY migrations ./migrations
COPY src ./src

RUN cargo build \
    --release \
    --locked \
    && test -x /workspace/target/release/gtd \
    && mkdir /runtime-data

FROM scratch

COPY --from=builder --chown=10001:10001 --chmod=0755 \
    /workspace/target/release/gtd /gtd
COPY --from=builder --chown=10001:10001 /runtime-data /data

# This runs inside scratch, so it also proves that /gtd has no runtime loader dependency.
RUN ["/gtd", "--help"]

USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
ENV GTD_DATABASE=/data/gtd.db
EXPOSE 4040
STOPSIGNAL SIGTERM

ENTRYPOINT ["/gtd"]
CMD ["server", "--bind", "0.0.0.0:4040", "--database", "/data/gtd.db"]
