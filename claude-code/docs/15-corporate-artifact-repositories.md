# 15 — Running behind a company artifact proxy (JFrog): `repos.env`

Every external artifact this repo downloads comes from one file, **`claude-code/repos.env`**:
container images, Maven dependencies, Gradle plugins, the Gradle distribution, the JDK
auto-download, Python packages and Ubuntu packages. To run the demos inside a company that
proxies everything through JFrog Artifactory, you don't edit any compose file, Dockerfile,
build script or shell script. You write **one override file** with your JFrog URLs, and every
project that follows this convention picks it up.

This doc is **binding** for the repo: a new module must not name a public registry or
repository anywhere except as a default in `repos.env` (section 8 has the checklist).

---

## 1. At your company, in three steps

```bash
# 1. one file with your JFrog URLs -- for this repo only ...
cp repos.local.env.example repos.local.env          # git-ignored; edit the URLs
#    ... or once for EVERY project (put the export in ~/.zshrc or ~/.bashrc):
mkdir -p ~/.config/jfrog && cp repos.local.env.example ~/.config/jfrog/repos.env
export REPOS_ENV=$HOME/.config/jfrog/repos.env

# 2. log in to the registry, check every endpoint, sync the Gradle wrapper
podman login mycompany.jfrog.io
scripts/repos.sh check        # effective values + where each came from + HTTP probe of each URL
scripts/repos.sh pull         # optional: pull every image through your registry

# 3. run anything, exactly as documented elsewhere
bash order-tree-recon/scripts/run_demo.sh
./gradlew build
```

All commands run from `claude-code/`. The run scripts (`run_demo.sh`, `run_tests.sh`, the e2e
and integration runners, `dh-connectors-compose.sh`) load the settings themselves. For a bare
command that the docs give without a script, prefix it with `scripts/repos.sh run`:

```bash
scripts/repos.sh run podman compose -f docker/docker-compose.yml up -d
scripts/repos.sh run pip install deephaven-server==42.4
```

## 2. The keys

| Key | Public default | Used by | JFrog repository it should point at |
|---|---|---|---|
| `GHCR_REGISTRY` | `ghcr.io` | `DEEPHAVEN_IMAGE` | Docker remote → `https://ghcr.io` |
| `DOCKERHUB_REGISTRY` | `docker.io` | `KAFKA_IMAGE`, `KAFKA_UI_IMAGE`, `MINIO_IMAGE`, `JRE_BASE_IMAGE` | Docker remote → Docker Hub |
| `DEEPHAVEN_IMAGE` | `${GHCR_REGISTRY}/deephaven/server:42.4` | every compose file, the derived Dockerfiles | (derived; pin outright if images are curated) |
| `KAFKA_IMAGE` | `${DOCKERHUB_REGISTRY}/apache/kafka:3.9.1` | `docker-compose.yml` | (derived) |
| `KAFKA_UI_IMAGE` | `${DOCKERHUB_REGISTRY}/kafbat/kafka-ui:v1.5.0` | `docker-compose.yml` | (derived) |
| `MINIO_IMAGE` | `${DOCKERHUB_REGISTRY}/minio/minio:RELEASE.2025-04-22T22-12-26Z` | `docker-compose.market-data.yml` | (derived) |
| `JRE_BASE_IMAGE` | `${DOCKERHUB_REGISTRY}/library/eclipse-temurin:21-jre-jammy` | dh-connectors images (`dockerBuildLocal`) | (derived) |
| `PIP_INDEX_URL` | `https://pypi.org/simple` | every `run_tests.sh` / e2e venv, the derived Dockerfiles | PyPI remote, `…/api/pypi/<repo>/simple` |
| `PIP_TRUSTED_HOST` | (off) | same | only if pip cannot verify the TLS certificate |
| `MAVEN_REPO_URL` | (Maven Central) | every Gradle dependency and plugin jar | Maven remote → Maven Central |
| `GRADLE_PLUGIN_REPO_URL` | (Gradle Plugin Portal) | Gradle plugin markers | Gradle remote → `https://plugins.gradle.org/m2` |
| `MAVEN_REPO_USERNAME` / `MAVEN_REPO_PASSWORD` | (none) | both repositories above | your JFrog user / identity token |
| `GRADLE_DISTRIBUTION_URL` | `https://services.gradle.org/distributions/gradle-9.2.1-bin.zip` | `gradlew` (synced into `gradle-wrapper.properties`) | Generic remote → `https://services.gradle.org/distributions` |
| `GRADLE_JDK_AUTO_DOWNLOAD` | `true` | the foojay toolchain resolver (`api.foojay.io`) | none: set `false`, install JDK 21 |
| `APT_MIRROR` | (Ubuntu archive) | `spring-boot.Dockerfile` (amd64 images) | Debian remote → `http://archive.ubuntu.com/ubuntu` |
| `APT_PORTS_MIRROR` | (Ubuntu ports) | `spring-boot.Dockerfile` (arm64 images, Apple silicon) | Debian remote → `http://ports.ubuntu.com/ubuntu-ports` |

Three things that are easy to get wrong:

- **Docker Hub official images** keep their `library/` path (`…/library/eclipse-temurin`). That
  is the canonical name on Docker Hub too, and what JFrog Docker Hub remotes expect.
- **JFrog Docker URLs** come in two styles. With the repository path method (the default),
  the registry is `mycompany.jfrog.io/ghcr-remote`. With subdomain or port mapping, it is
  `ghcr-remote.mycompany.jfrog.io` or `mycompany.jfrog.io:5001`. Either works as the value of
  `GHCR_REGISTRY`.
- **amd64 vs arm64 Ubuntu** packages live in different archives (`archive.ubuntu.com/ubuntu`
  vs `ports.ubuntu.com/ubuntu-ports`). podman on an Apple-silicon Mac builds arm64 images,
  so it needs `APT_PORTS_MIRROR`. This only matters for the dh-connectors images.

## 3. Precedence and file locations

First definition wins:

1. **the environment**: anything already exported (`DEEPHAVEN_IMAGE=… bash run_demo.sh`);
2. **the override file**: `$REPOS_ENV` if set, else `claude-code/repos.local.env` if it exists;
3. **`claude-code/repos.env`**: the public defaults (tracked, don't edit for your company).

The override is read **before** `repos.env`. So `GHCR_REGISTRY` set in the override also flows
into `DEEPHAVEN_IMAGE=${GHCR_REGISTRY}/…` in `repos.env`, and two registry lines re-point every
image. List only what differs; everything else keeps its public default.

File format, identical for the shell loader and for Gradle:
- `KEY=value` lines, with an optional leading `export `;
- `${KEY}` references (to earlier keys or the environment);
- `#` comments on their own line (a `#` inside a value is kept);
- optional surrounding quotes, CRLF line endings tolerated;
- an empty value means "public default" / "off".

The files are **parsed, never sourced**, so a config file cannot run code.

`scripts/repos.sh show` prints every key with its value and its source (`environment`,
`override`, `repos.env`), masking passwords, tokens and credentials embedded in URLs.

## 4. Who reads it

| Consumer | How |
|---|---|
| shell scripts | `source "…/scripts/repos.sh"` right after `set -euo pipefail` in every run script. It exports the keys, so pip (`PIP_INDEX_URL`), compose interpolation and child processes see them. Sourcing also syncs the Gradle wrapper URL. |
| podman compose | every `image:` is `${KEY:-<public default>}`, and derived images get `build.args` (`DEEPHAVEN_IMAGE`, `PIP_INDEX_URL`, `PIP_TRUSTED_HOST`). With no loader, the public defaults apply, so a bare `podman compose …` still works outside the company. |
| Dockerfiles | the base image is an `ARG` with the public default. pip reads the `PIP_*` build args from the environment, and the dh-connectors image rewrites its apt sources to `APT_MIRROR` / `APT_PORTS_MIRROR` when set. |
| Gradle | `gradle/repos.settings.gradle.kts`, applied from the `pluginManagement` block of both `settings.gradle.kts` and `build-logic/settings.gradle.kts`, parses the same files with the same rules. It sets the plugin and dependency repositories and publishes the map as `gradle.extra["repos"]`, which `settings.gradle.kts` uses for the foojay switch and `dockerBuildLocal` uses for its `--build-arg`s. |
| Gradle wrapper | the wrapper reads only `gradle/wrapper/gradle-wrapper.properties`, so `scripts/repos.sh` (sourced, `check`, `run`) rewrites its `distributionUrl` to `GRADLE_DISTRIBUTION_URL`. |

### Gradle specifics

- **No module declares repositories.** `dependencyResolutionManagement` uses
  `RepositoriesMode.FAIL_ON_PROJECT_REPOS`, so a module that adds `repositories {
  mavenCentral() }` fails the build ("repository 'MavenRepo' was added by build file …")
  instead of quietly bypassing your mirror.
- **Plugin resolution** tries the Maven repository first (most plugin implementation jars
  live there), then the plugin repository for the plugin markers.
- **Credentials** are read from the environment, the override file, or
  `~/.gradle/gradle.properties` (`MAVEN_REPO_USERNAME=…`, `MAVEN_REPO_PASSWORD=…`), in that
  order. The last keeps the token out of every project folder.
- **The wrapper sync** modifies a tracked file. To stop git from showing it:
  `git update-index --skip-worktree gradle/wrapper/gradle-wrapper.properties`.
- **The JDK**: with `GRADLE_JDK_AUTO_DOWNLOAD=false` the foojay resolver isn't applied. Gradle
  uses a locally installed JDK 21 and, if there is none, fails with "no matching toolchain"
  rather than calling `api.foojay.io`. Gradle 9 itself also needs a JDK 17+ to start.

## 5. Credentials and certificates

- **Container registry:** `podman login mycompany.jfrog.io` once (credentials live in podman's
  auth file, never in this repo).
- **Maven / Gradle:** see the Gradle specifics above.
- **pip:** if your PyPI remote needs a login, use `~/.netrc` (`machine mycompany.jfrog.io
  login … password …`) or a `PIP_INDEX_URL` with credentials, and put the latter only in your
  override file (git-ignored) or the environment. `scripts/repos.sh show` masks it.
- **Company TLS CA:** if JFrog uses an internal CA, the host tools need to trust it (macOS
  keychain / Linux CA store, the JDK truststore for Gradle, and the podman machine for image
  pulls). Image builds that run pip inside a container need the CA in the image or
  `PIP_TRUSTED_HOST`. These are machine setup, not repo settings.

## 6. Verifying a setup

```bash
scripts/repos.sh check   # every key + source; HTTP probe: ok / AUTH (401/403) / BUSY (429) / FAIL
scripts/repos.sh pull    # podman pull of every *_IMAGE: proves registry paths and login
```

## 7. What was verified (and what wasn't)

The sandbox this was built in has no container runtime. Maven Central rate-limits it
(HTTP 429) and `api.foojay.io` is blocked, which is roughly what a locked-down company
network looks like.

- **Gradle, full build through a non-default Maven URL.** `assemble compileTestJava` with
  `MAVEN_REPO_URL` set to Google's Maven Central mirror and `GRADLE_JDK_AUTO_DOWNLOAD=false`:
  BUILD SUCCESSFUL. All 681 downloads came from the configured URL, none from Maven Central.
- **Gradle checks.** An init-script probe showed:
  - the plugin and dependency repository lists per mode;
  - foojay applied / not applied per the switch;
  - an unreachable `GRADLE_PLUGIN_REPO_URL` is what plugin resolution then fails on;
  - a module re-adding `mavenCentral()` fails the build.

  With no override, the repository lists are exactly Maven Central and the Plugin Portal,
  as before.
- **pip.** With an unreachable `PIP_INDEX_URL`, a `run_tests.sh` started directly and one
  started by Gradle (`:order-tree-recon:pytest`) both report `Looking in indexes:
  <that URL>`. With the default, 81 tests pass.
- **Compose.** `docker compose config` (Compose v5.6) renders the public images with no
  override, and the JFrog paths plus build args with one.
- **Dockerfiles.** The apt rewrite was run with `sh` on classic and deb822 sources files for
  amd64 and arm64. An actual `podman build` / `podman pull` through JFrog was **not** run
  here.
- **The loader.** Tested for precedence, `REPOS_ENV`, quotes, CRLF, `export`, invalid lines,
  masking, a missing `REPOS_ENV` failing fast, and the idempotent wrapper sync. It is
  written for bash 3.2 (macOS) but was only run on bash 5.

## 8. Adding a project or module (the convention)

So that one override file keeps working for every new demo:

1. **Images:** in compose, `image: ${FOO_IMAGE:-<public image:tag>}`, and add
   `FOO_IMAGE=${DOCKERHUB_REGISTRY}/…:tag` (or `${GHCR_REGISTRY}`) to `repos.env`. Write
   images fully qualified (`docker.io/…`, with `library/` for official images).
2. **Dockerfiles:** `ARG BASE_IMAGE=<public default>` + `FROM ${BASE_IMAGE}`. Any
   `pip install` gets `ARG PIP_INDEX_URL` / `PIP_TRUSTED_HOST` (passed from compose
   `build.args`), and any `apt-get` honours `APT_MIRROR` / `APT_PORTS_MIRROR`.
3. **Scripts** that pip-install, pull images, run compose or call `./gradlew`: add the
   two-line `source "…/scripts/repos.sh"` right after `set -euo pipefail`.
4. **Gradle modules:** no `repositories {}` block (the build fails if you add one).
5. **A new upstream** (another registry, npm, …): add a key with the public default to
   `repos.env`, a commented JFrog example to `repos.local.env.example`, a row to section 2,
   and read it in the place that downloads.

## 9. Out of scope

- **The AMPS broker image** (`AMPS_IMAGE`, remote-URI demo) is proprietary and already
  user-supplied.
- **Host tools** (podman, a JDK, python3) are installed by your company's usual means.
- **The running Deephaven server** downloads nothing at startup: `deephaven.ui` and
  `deephaven.plot.express` are bundled in the image.
