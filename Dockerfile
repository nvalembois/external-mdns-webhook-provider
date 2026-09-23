ARG USER_ID=1000
ARG USER_NAME=webhook
ARG USER_GECOS='webhook_provider daemon'
ARG GROUP_ID=1000
ARG GROUP_NAME=webhook

# build stage
FROM docker.io/library/rust:1.94.1-alpine3.20 AS build

WORKDIR /tmp

ARG USER_ID USER_NAME USER_GECOS GROUP_ID GROUP_NAME
RUN addgroup -S -g $GROUP_ID $GROUP_NAME \
 && adduser -S -u $USER_ID -G $GROUP_NAME -D -H -h / -s /sbin/nologin -g "$USER_GECOS" $USER_NAME \
 && addgroup $USER_NAME $GROUP_NAME

COPY Cargo.toml Cargo.lock /tmp/
COPY src /tmp/src/
RUN apk add --no-cache musl-dev build-base && \
    cargo build --release --bin webhook_provider && \
    mv target/release/webhook_provider . && \
    cargo clean && \
    apk del --no-cache -r musl-dev build-base

# distroless image
FROM scratch

COPY --from=build /etc/passwd /etc/shadow /etc/group /etc/
COPY --from=build /tmp/webhook_provider /

ARG USER_ID
USER $USER_ID

ENTRYPOINT [ "/webhook_provider" ]
