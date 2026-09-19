# ONE shared Dockerfile for every connector application.
#
# It can be generic because the build context is generic: the dh.connector-app
# convention plugin's stageDockerContext task stages exactly `application.jar` +
# this file into <module>/build/docker, so there is no ARG for the app, no
# repo-root context, and no per-app copies to drift apart.
#
#   ./gradlew :dh-connectors:connector-app:dockerBuildLocal
#   ./gradlew :dh-connectors:apps:<name>:dockerBuildLocal
#
# (both run `podman build --format docker <module>/build/docker` under the hood;
# --format docker keeps the HEALTHCHECK, which the OCI image format drops).

ARG BASE_IMAGE=eclipse-temurin:21-jre-jammy

FROM ${BASE_IMAGE} AS extract
WORKDIR /workspace
COPY application.jar .
# Boot 3.3+ "tools" jar mode: split into cache-friendly layers so the dependency
# layers stay byte-identical across app rebuilds.
RUN java -Djarmode=tools -jar application.jar extract --layers --launcher --destination extracted

FROM ${BASE_IMAGE}
# wget for the healthcheck; temurin's jammy JRE image ships without it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends wget \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Ordered least -> most volatile for layer cache hits.
COPY --from=extract /workspace/extracted/dependencies/          ./
COPY --from=extract /workspace/extracted/spring-boot-loader/    ./
COPY --from=extract /workspace/extracted/snapshot-dependencies/ ./
COPY --from=extract /workspace/extracted/application/           ./

# The parity contract: configuration arrives ONLY as files under /app/config
# (common first, instance second — later locations win). The directories exist
# so the bare image boots with the baked defaults when nothing is mounted.
RUN mkdir -p /app/config/common /app/config/instance && chown -R 1001:0 /app
ENV SPRING_CONFIG_ADDITIONAL_LOCATION="file:/app/config/common/,file:/app/config/instance/"

# Arrow 18 reaches into java.nio internals for its off-heap allocator; same flag
# the gradle test/bootRun tasks pass.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"

USER 1001
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=20 \
  CMD ["sh", "-c", "wget -qO- http://localhost:8080/actuator/health/readiness || exit 1"]

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
