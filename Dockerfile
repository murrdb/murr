FROM debian:bookworm-20260223-slim

ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --no-log-init murr

COPY docker-bin/linux/${TARGETARCH}/murr /usr/bin/murr

USER murr

EXPOSE 8080 8081

ENTRYPOINT ["tini", "--", "/usr/bin/murr"]
