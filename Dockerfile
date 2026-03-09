FROM debian:bookworm-20260223-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-20260223-slim

RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --no-log-init murr

COPY --from=builder /build/target/release/murr /usr/bin/murr

USER murr

EXPOSE 8080 8081

ENTRYPOINT ["tini", "--", "/usr/bin/murr"]
