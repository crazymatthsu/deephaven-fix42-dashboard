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

# The base image and the apt mirror come from claude-code/repos.env (JRE_BASE_IMAGE,
# APT_MIRROR; docs/15) -- dockerBuildLocal passes them as --build-arg. The defaults are the
# public ones, so a plain `podman build` still works.
ARG BASE_IMAGE=docker.io/library/eclipse-temurin:21-jre-jammy

FROM ${BASE_IMAGE} AS extract
WORKDIR /workspace
COPY application.jar .
# Boot 3.3+ "tools" jar mode: split into cache-friendly layers so the dependency
# layers stay byte-identical across app rebuilds.
RUN java -Djarmode=tools -jar application.jar extract --layers --launcher --destination extracted

FROM ${BASE_IMAGE}
# Empty = Ubuntu's own archives. APT_MIRROR replaces archive/security.ubuntu.com/ubuntu
# (amd64 images); APT_PORTS_MIRROR replaces ports.ubuntu.com/ubuntu-ports (arm64 images, e.g.
# podman on Apple silicon) -- they are different archives, so they need separate mirrors.
# Both the classic and the deb822 sources file are rewritten, whichever the base image has.
ARG APT_MIRROR=
ARG APT_PORTS_MIRROR=
# wget for the healthcheck; temurin's jammy JRE image ships without it.
RUN for f in /etc/apt/sources.list /etc/apt/sources.list.d/ubuntu.sources; do \
      [ -f "$f" ] || continue; \
      if [ -n "$APT_MIRROR" ]; then \
        sed -i -E "s#https?://(archive|security)\.ubuntu\.com/ubuntu/?#${APT_MIRROR%/}/#g" "$f"; \
      fi; \
      if [ -n "$APT_PORTS_MIRROR" ]; then \
        sed -i -E "s#https?://ports\.ubuntu\.com/ubuntu-ports/?#${APT_PORTS_MIRROR%/}/#g" "$f"; \
      fi; \
    done \
 && apt-get update \
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
