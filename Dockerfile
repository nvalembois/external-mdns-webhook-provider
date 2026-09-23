FROM docker.io/library/rust:1.94.1-alpine3.20 AS build

COPY Cargo.toml Cargo.lock /tmp/
COPY src /tmp/src/

WORKDIR /tmp

RUN set -e && \
  apk add --no-cache musl-dev build-base && \
  cargo build --release --bin webhook_provider

FROM scratch

COPY --from=build /tmp/target/release/webhook_provider /

USER 10000
ENTRYPOINT [ "/webhook_provider" ]