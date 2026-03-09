FROM ubuntu:noble-20260210.1 AS builder

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

FROM ubuntu:noble-20260210.1

COPY --from=builder /build/target/release/murr /usr/bin/murr

EXPOSE 8080 8081

ENTRYPOINT ["/usr/bin/murr"]
