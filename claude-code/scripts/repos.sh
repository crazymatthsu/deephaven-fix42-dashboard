#!/usr/bin/env bash
#
# The one loader for the artifact-repository settings in repos.env (docs/15).
#
#   source scripts/repos.sh            # in a script: export the settings (+ sync the wrapper URL)
#   scripts/repos.sh show              # print the effective settings and where each came from
#   scripts/repos.sh check             # show, sync the Gradle wrapper URL, probe every HTTP endpoint
#   scripts/repos.sh pull              # podman pull every *_IMAGE (proves registry paths + login)
#   scripts/repos.sh run CMD [ARG...]  # load, sync the wrapper URL, then exec CMD, e.g.
#                                      #   scripts/repos.sh run podman compose -f docker/docker-compose.yml up -d
#                                      #   scripts/repos.sh run ./gradlew build
#
# Precedence, first definition wins:
#   1. the environment (anything already exported)
#   2. the override file: $REPOS_ENV if set, else claude-code/repos.local.env if present
#   3. claude-code/repos.env (public defaults)
# The override is read before repos.env, so ${GHCR_REGISTRY}-style references in repos.env
# expand to the overridden registry.
#
# The files are PARSED, never sourced: a config file cannot run code. Format: KEY=value,
# optional leading `export `, ${KEY} references, '#' comments on their own line, optional
# surrounding quotes. A key with an empty value is left unset (= the public default).
#
# Written for bash 3.2 (macOS /bin/bash): no associative arrays, no mapfile.

REPOS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOS_DEFAULTS_FILE="$REPOS_ROOT/repos.env"

# Expand ${NAME} references in $1 from the (already exported) environment.
_repos_expand() {
  local rest="$1" out="" name open='${' close='}'
  while :; do
    case "$rest" in
      *"$open"*"$close"*) ;;
      *) printf '%s' "$out$rest"; return 0 ;;
    esac
    out="$out${rest%%"$open"*}"
    rest="${rest#*"$open"}"
    name="${rest%%"$close"*}"
    rest="${rest#*"$close"}"
    out="$out$(printenv "$name" 2>/dev/null || true)"
  done
}

# Parse one file; keys already defined (by the environment or an earlier file) are skipped.
_repos_load_file() {
  local file="$1" origin="$2" line key val
  [ -f "$file" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    case "$line" in '' | '#'*) continue ;; esac
    line="${line#export }"
    case "$line" in
      *=*) ;;
      *) echo "repos: ignoring line without '=' in $file: $line" >&2; continue ;;
    esac
    key="${line%%=*}"
    key="${key%"${key##*[![:space:]]}"}"
    val="${line#*=}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    case "$key" in
      '' | [0-9]* | *[!A-Za-z0-9_]*) echo "repos: ignoring invalid key '$key' in $file" >&2; continue ;;
    esac
    case "$val" in
      \"*\") val="${val#\"}"; val="${val%\"}" ;;
      \'*\') val="${val#\'}"; val="${val%\'}" ;;
    esac
    case " $_REPOS_KEYS " in *" $key "*) continue ;; esac
    _REPOS_KEYS="$_REPOS_KEYS $key"
    if printenv "$key" >/dev/null 2>&1; then
      printf -v "_REPOS_FROM_$key" '%s' "environment"
      continue
    fi
    val="$(_repos_expand "$val")"
    printf -v "_REPOS_FROM_$key" '%s' "$origin"
    if [ -n "$val" ]; then export "$key=$val"; else unset "$key"; fi
  done <"$file"
  return 0
}

# Load override + defaults into the environment. Returns 1 if REPOS_ENV names a missing file.
repos_load() {
  _REPOS_KEYS=""
  REPOS_OVERRIDE_FILE=""
  if [ -n "${REPOS_ENV:-}" ]; then
    if [ ! -f "$REPOS_ENV" ]; then
      echo "repos: REPOS_ENV=$REPOS_ENV does not exist" >&2
      return 1
    fi
    REPOS_OVERRIDE_FILE="$REPOS_ENV"
  elif [ -f "$REPOS_ROOT/repos.local.env" ]; then
    REPOS_OVERRIDE_FILE="$REPOS_ROOT/repos.local.env"
  fi
  if [ -n "$REPOS_OVERRIDE_FILE" ]; then
    _repos_load_file "$REPOS_OVERRIDE_FILE" "override"
  fi
  _repos_load_file "$REPOS_DEFAULTS_FILE" "repos.env"
}

# Hide passwords/tokens and credentials embedded in URLs.
_repos_mask() {
  case "$1" in
    *PASSWORD* | *TOKEN* | *SECRET*) printf '****' ;;
    *) printf '%s' "$2" | sed -E 's#(://[^:/@]+):[^@/]+@#\1:****@#' ;;
  esac
}

repos_show() {
  local key val from
  echo "override file: ${REPOS_OVERRIDE_FILE:-(none -- public defaults; see repos.local.env.example)}"
  for key in $_REPOS_KEYS; do
    val="$(printenv "$key" 2>/dev/null || true)"
    eval "from=\${_REPOS_FROM_$key:-}"
    printf '  %-26s %-70s %s\n' "$key" "$(_repos_mask "$key" "${val:-(unset: public default / off)}")" "[$from]"
  done
}

# Point gradle/wrapper/gradle-wrapper.properties at GRADLE_DISTRIBUTION_URL (no-op if equal).
repos_sync_wrapper() {
  local props="$REPOS_ROOT/gradle/wrapper/gradle-wrapper.properties" want esc current tmp
  want="${GRADLE_DISTRIBUTION_URL:-}"
  [ -n "$want" ] && [ -f "$props" ] || return 0
  esc="$(printf '%s' "$want" | sed 's/:/\\:/g')"
  current="$(sed -n 's/^distributionUrl=//p' "$props")"
  [ "$current" = "$esc" ] && return 0
  tmp="$props.tmp.$$"
  awk -v url="$esc" '/^distributionUrl=/ { print "distributionUrl=" url; next } { print }' "$props" >"$tmp" \
    && mv "$tmp" "$props"
  echo "repos: gradle wrapper now downloads $want (gradle/wrapper/gradle-wrapper.properties)" >&2
}

# HTTP status of a URL ('000' = unreachable). HEAD first, GET if the server refuses HEAD.
_repos_http() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 -I "$1" 2>/dev/null)" || code="000"
  case "$code" in 405 | 501) code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 "$1" 2>/dev/null)" || code="000" ;; esac
  printf '%s' "$code"
}

repos_check() {
  local failed=0 label url code
  repos_show
  repos_sync_wrapper
  echo
  echo "probing endpoints (2xx/3xx = ok, 401/403 = reachable but needs credentials):"
  while IFS='|' read -r label url; do
    [ -n "$url" ] || continue
    code="$(_repos_http "$url")"
    case "$code" in
      2* | 3*) printf '  ok    %s  %s (%s)\n' "$code" "$label" "$url" ;;
      401 | 403) printf '  AUTH  %s  %s (%s) -- add credentials\n' "$code" "$label" "$url" ;;
      429) printf '  BUSY  %s  %s (%s) -- rate-limited, retry later\n' "$code" "$label" "$url"; failed=1 ;;
      *) printf '  FAIL  %s  %s (%s)\n' "$code" "$label" "$url"; failed=1 ;;
    esac
  done <<EOF
pip index|${PIP_INDEX_URL:-https://pypi.org/simple}/
maven repo|${MAVEN_REPO_URL:-https://repo.maven.apache.org/maven2}/
gradle plugin repo|${GRADLE_PLUGIN_REPO_URL:-https://plugins.gradle.org/m2}/
gradle distribution|${GRADLE_DISTRIBUTION_URL:-}
apt mirror|${APT_MIRROR:+$APT_MIRROR/}
EOF
  echo
  echo "container images (test pulls with: scripts/repos.sh pull):"
  for label in $(repos_images); do printf '  %s\n' "$(printenv "$label")"; done
  return "$failed"
}

# The *_IMAGE keys from the files, in file order.
repos_images() {
  local key
  for key in $_REPOS_KEYS; do
    case "$key" in *_IMAGE) printenv "$key" >/dev/null 2>&1 && echo "$key" ;; esac
  done
}

repos_pull() {
  local key failed=0
  for key in $(repos_images); do
    echo "== $key"
    podman pull "$(printenv "$key")" || failed=1
  done
  return "$failed"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  # Executed: a small CLI.
  set -uo pipefail
  repos_load || exit 1
  cmd="${1:-show}"
  [ $# -gt 0 ] && shift
  case "$cmd" in
    show) repos_show ;;
    check) repos_check ;;
    pull) repos_pull ;;
    run)
      [ $# -gt 0 ] || { echo "usage: $0 run CMD [ARG...]" >&2; exit 2; }
      repos_sync_wrapper
      exec "$@"
      ;;
    -h | --help | help) sed -n '2,25p' "$0" ;;
    *) echo "usage: $0 [show|check|pull|run CMD...]" >&2; exit 2 ;;
  esac
else
  # Sourced: load, and keep the Gradle wrapper pointed at GRADLE_DISTRIBUTION_URL so a script
  # that calls ./gradlew downloads Gradle from the same place (silent when already in sync).
  repos_load && repos_sync_wrapper
fi
