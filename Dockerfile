# Build and run stages are separate so the shipped image has no compiler, no Maven cache, and no
# source. It is also why the image is pinned by digest-able tags rather than :latest: an image that
# rebuilds differently tomorrow is not a deployment artifact.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

# Dependencies first, so editing a source file does not re-download the world.
COPY pom.xml ./
COPY keel-proto/pom.xml keel-proto/
COPY keel-raft/pom.xml keel-raft/
COPY keel-storage/pom.xml keel-storage/
COPY keel-kv/pom.xml keel-kv/
COPY keel-testkit/pom.xml keel-testkit/
COPY keel-node/pom.xml keel-node/
RUN mvn -B -ntp -q dependency:go-offline -DskipTests || true

COPY . .
# Tests run in CI. Running them again here would double the image build time to re-prove the same
# commit, and a failure at this point is a build problem rather than a code problem.
RUN mvn -B -ntp package -DskipTests

FROM eclipse-temurin:21-jre
LABEL org.opencontainers.image.title="keel" \
      org.opencontainers.image.description="Linearizable distributed key-value store" \
      org.opencontainers.image.source="https://github.com/jason-te-sde/keel" \
      org.opencontainers.image.licenses="MIT"

# Unprivileged, and owning only its data directory. A consensus node has no reason to be root.
RUN useradd --system --create-home --uid 10001 keel
COPY --from=build /src/keel-node/target/keel.jar /opt/keel/keel.jar
RUN mkdir -p /var/lib/keel && chown -R keel:keel /var/lib/keel
USER keel
VOLUME ["/var/lib/keel"]

# 9001 peer and client traffic, 9101 metrics and health.
EXPOSE 9001 9101

# Container memory is the limit that matters, not a number baked into the image.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"

# Readiness rather than liveness: a container that is up but cannot serve a read should not receive
# traffic. Orchestrators that distinguish the two should use /healthz for liveness as well.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
    CMD ["/bin/sh", "-c", "exec 3<>/dev/tcp/127.0.0.1/9101 && printf 'GET /readyz HTTP/1.0\\r\\n\\r\\n' >&3 && head -1 <&3 | grep -q '200'"]

ENTRYPOINT ["/bin/sh", "-c", "exec java $JAVA_OPTS -cp /opt/keel/keel.jar io.keel.node.Keeld \"$@\"", "--"]
CMD ["--help"]
