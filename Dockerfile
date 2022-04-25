FROM rust as builder

RUN mkdir /opt/app
WORKDIR /opt/app

COPY src/ ./src/
COPY Cargo.toml ./
COPY Cargo.lock ./

RUN apt-get update && apt-get install -y cmake clang
RUN cargo clean && \
    cargo build -vv --release

FROM debian:buster-slim

ARG APP=/usr/src/app

RUN apt-get update && apt-get install -y libssl-dev

ENV APP_USER=appuser

RUN groupadd $APP_USER \
    && useradd -g $APP_USER $APP_USER \
    && mkdir -p ${APP}

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /opt/app/target/release/fdk-mqa-property-checker ${APP}/fdk-mqa-property-checker

RUN chown -R $APP_USER:$APP_USER ${APP}

USER $APP_USER
WORKDIR ${APP}

CMD ./fdk-mqa-property-checker --brokers "$BROKERS" --schema-registry "$SCHEMA_REGISTRY"

