# syntax=docker/dockerfile:1.7
#
# Runtime image for opcdapter. Used as a template by the Micronaut Gradle
# plugin: `./gradlew buildLayers dockerfile` stages the host-built jar into
# build/docker/main/layers/ and copies this file to build/docker/main/ (the
# build context); `just docker` then runs `docker build` there. The jar is
# built on the host, so no QEMU emulation is involved when cross-building
# for linux/amd64.
#
# Generated Java protocol sources are compiled with the server.
# Build:  just docker
# Load on the offline host:
#         docker load -i opcdapter-<tag>.tar
#         docker run -d --name opcdapter -p 127.0.0.1:50051:50051 \
#             -v opcdapter-logs:/app/logs \
#             opcdapter:<tag>

FROM eclipse-temurin:25-jre-noble

ARG VERSION=dev
ARG COMMIT=none
ARG DATETIME=unknown

LABEL org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${DATETIME}" \
      org.opencontainers.image.title="opcdapter"

ENV APP_VERSION="${VERSION}" \
    APP_COMMIT="${COMMIT}" \
    APP_DATETIME="${DATETIME}"

WORKDIR /app

# Seed a writable log directory without a target-platform RUN layer. This keeps
# linux/amd64 cross-builds architecture-independent on Apple Silicon.
COPY --chown=10001:10001 <<EOF /app/logs/.keep
opcdapter runtime log directory
EOF

# Layered application staged by buildLayers; application.jar's manifest
# Class-Path references libs/ and resources/ relative to /app.
COPY --link --chown=10001:10001 layers/libs /app/libs
COPY --link --chown=10001:10001 layers/app /app/
COPY --link --chown=10001:10001 layers/resources /app/resources

USER 10001:10001

EXPOSE 50051

ENV MALLOC_ARENA_MAX=2 \
    MALLOC_TRIM_THRESHOLD_=65536 \
    HOME=/tmp \
    JAVA_OPTS="-Xms512m -Xmx512m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/logs/ -Xlog:gc*:file=/app/logs/gc.log:time,uptime,level,tags:filecount=5,filesize=10m" \
    TZ=Asia/Shanghai \
    OPCDAPTER_GRPC_SERVER_HOST=0.0.0.0
# Debug: add to JAVA_OPTS to detect Netty direct buffer leaks (~10% overhead):
#   -Dio.netty.leakDetection.level=ADVANCED
#   -XX:MaxDirectMemorySize=64m

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["bash", "-ec", "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /health HTTP/1.0\\r\\nHost: localhost\\r\\n\\r\\n' >&3; grep -q '\"status\":\"UP\"' <&3"]

# -Dopcdapter.log.dir routes logback's rolling file appender into /app/logs
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Dopcdapter.log.dir=/app/logs -jar /app/application.jar"]
