# Working in this repository

## External artifact sources go through `claude-code/repos.env`

The demos must run unchanged inside a company that proxies every download through JFrog
Artifactory. The contract is [`claude-code/docs/15-corporate-artifact-repositories.md`](claude-code/docs/15-corporate-artifact-repositories.md):
every container registry, Maven / Gradle repository, Gradle distribution, pip index and apt
mirror comes from a key in `claude-code/repos.env`, and users re-point all of them with one
override file. Never hardcode a public source anywhere else. When adding or changing a module:

- **Compose images:** `image: ${FOO_IMAGE:-<fully qualified public image:tag>}`, and add
  `FOO_IMAGE=${DOCKERHUB_REGISTRY}/...` (or `${GHCR_REGISTRY}/...`) to `repos.env`. Docker Hub
  official images keep `library/`.
- **Dockerfiles:** `ARG BASE_IMAGE=<public default>` then `FROM ${BASE_IMAGE}`. A `pip install`
  takes `ARG PIP_INDEX_URL` / `ARG PIP_TRUSTED_HOST` from compose `build.args`, and an
  `apt-get` honours `APT_MIRROR` / `APT_PORTS_MIRROR` (see `dh-connectors/docker/spring-boot.Dockerfile`).
- **Shell scripts** that pip-install, pull or build images, run compose or call `./gradlew`
  source the loader right after `set -euo pipefail`:
  `source "$(cd "$(dirname "${BASH_SOURCE[0]}")/<rel>" && pwd)/scripts/repos.sh"`.
- **Gradle modules** declare no `repositories {}`. Repositories come from
  `claude-code/gradle/repos.settings.gradle.kts`, and `FAIL_ON_PROJECT_REPOS` fails the build
  if a module adds one.
- **A new upstream** (another registry, npm, ...) gets a key with its public default in
  `repos.env`, a commented JFrog example in `repos.local.env.example`, and a row in doc 15's
  key table.

Check a change with `claude-code/scripts/repos.sh check`.

## Scripts run on macOS, Linux and Windows Git Bash

See doc 15 section 10.

- **Shell compatibility:** bash 3.2 (macOS `/bin/bash`) and BSD tools. No associative arrays,
  `mapfile` or `${x,,}`; no GNU-only flags (`sed -i` without a suffix, `readlink -f`, `date -d`,
  `grep -P`, `timeout`).
- **Python:** find it with `find_python3 <min>` and a venv's interpreter with
  `venv_python <dir>` (both in `claude-code/scripts/repos.sh`). Never hardcode `python3` or
  `.venv/bin/`.
- **Container paths:** a container-internal absolute path passed as a command-line argument
  (`podman exec <ctr> /opt/...`) needs `MSYS_NO_PATHCONV=1` in front, or Git Bash rewrites it.
- **Gradle:** `Exec` tasks that run a script pass `file.invariantSeparatorsPath`.
- **Line endings:** `.gitattributes` keeps every text file LF on checkout; only `.bat` and
  `.cmd` are CRLF.

