#!/usr/bin/env bash
#
# Copy the order tree reconciliation module, plus everything it needs to run on its own, into
# DEST: a new directory, or the root of another project (doc 14 section 13).
#
#   bash order-tree-recon/scripts/export_standalone.sh ~/work/order-tree
#   bash order-tree-recon/scripts/export_standalone.sh --force ~/work/order-tree   # overwrite
#
# DEST gets the same layout as claude-code/, so every relative path in the scripts, the
# compose file and the docs keeps working:
#
#   order-tree-recon/       the package, tests, fix-samples/, run_tests.sh, scripts/run_demo.sh
#   docker/                 docker-compose.order-tree.yml, apps/_lib/loader.py, apps/order-tree-recon/
#   scripts/repos.sh        the artifact-repository loader (docs/15)
#   repos.env               public defaults (+ repos.local.env.example: your JFrog override)
#   docs/14-*.md, docs/15-*.md
#   .gitattributes, .gitignore, README.md (README.order-tree-recon.md if DEST has a README)
#
# Left behind: build.gradle.kts (it plugs into this repo's Gradle build; bash run_tests.sh does
# the same job), .venv and caches, and your repos.local.env (it may hold credentials; copy it
# yourself if DEST is not going into a shared git repository).
#
# An existing order-tree-recon/ in DEST is an error without --force. Other files that
# already exist (repos.env, scripts/repos.sh, .gitattributes, ...) are kept unless --force;
# .gitignore only gets the missing lines appended.
set -euo pipefail

FORCE=0
DEST=""
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h | --help) sed -n '2,25p' "$0"; exit 0 ;;
    -*) echo "unknown option: $arg" >&2; exit 2 ;;
    *) [ -z "$DEST" ] || { echo "usage: $0 [--force] DEST" >&2; exit 2; }; DEST="$arg" ;;
  esac
done
[ -n "$DEST" ] || { echo "usage: $0 [--force] DEST" >&2; exit 2; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$MODULE/.." && pwd)"   # claude-code/
# .gitattributes: claude-code/ has none (it is at the repository root); an exported copy has one.
GITATTRIBUTES="$ROOT/.gitattributes"
[ -f "$GITATTRIBUTES" ] || GITATTRIBUTES="$(cd "$ROOT/.." && pwd)/.gitattributes"

# Git Bash: accept C:\path\to\dest as well as /c/path/to/dest.
if command -v cygpath >/dev/null 2>&1; then DEST="$(cygpath -u "$DEST")"; fi
mkdir -p "$DEST"
DEST="$(cd "$DEST" && pwd)"
case "$DEST/" in
  "$ROOT/"*) echo "DEST must be outside $ROOT" >&2; exit 2 ;;
esac

if [ -e "$DEST/order-tree-recon" ] && [ "$FORCE" -ne 1 ]; then
  echo "$DEST/order-tree-recon already exists (re-run with --force to overwrite)" >&2
  exit 1
fi

# copy SRC_DIR REL [REL...] -- tar keeps the relative paths; works with GNU tar (Linux, Git
# Bash) and bsdtar (macOS). --exclude must come before the paths for bsdtar.
copy_tree() {
  local src="$1"
  shift
  tar -C "$src" -cf - \
    --exclude '.venv' --exclude '__pycache__' --exclude '.pytest_cache' --exclude '*.egg-info' \
    --exclude '*.pyc' --exclude 'build.gradle.kts' \
    "$@" | tar -C "$DEST" -xf -
}

# keep_or_copy SRC_FILE REL -- copy unless DEST already has REL (and no --force).
keep_or_copy() {
  local src="$1" rel="$2"
  if [ -e "$DEST/$rel" ] && [ "$FORCE" -ne 1 ]; then
    echo "  kept     $rel (exists; --force overwrites)"
    return 0
  fi
  mkdir -p "$(dirname "$DEST/$rel")"
  cp "$src" "$DEST/$rel"
  echo "  copied   $rel"
}

echo "exporting order-tree-recon to $DEST"
rm -rf "$DEST/order-tree-recon"
copy_tree "$ROOT" order-tree-recon
echo "  copied   order-tree-recon/"
copy_tree "$ROOT" docker/apps/order-tree-recon
echo "  copied   docker/apps/order-tree-recon/"
keep_or_copy "$ROOT/docker/docker-compose.order-tree.yml" docker/docker-compose.order-tree.yml
keep_or_copy "$ROOT/docker/apps/_lib/loader.py" docker/apps/_lib/loader.py
keep_or_copy "$ROOT/scripts/repos.sh" scripts/repos.sh
keep_or_copy "$ROOT/repos.env" repos.env
keep_or_copy "$ROOT/repos.local.env.example" repos.local.env.example
for doc in "$ROOT"/docs/14-*.md "$ROOT"/docs/15-*.md; do
  keep_or_copy "$doc" "docs/$(basename "$doc")"
done
if [ -f "$GITATTRIBUTES" ]; then keep_or_copy "$GITATTRIBUTES" .gitattributes; fi

touch "$DEST/.gitignore"
# A file without a final newline would glue our first line onto its last one.
if [ -s "$DEST/.gitignore" ] && [ -n "$(tail -c 1 "$DEST/.gitignore")" ]; then echo >>"$DEST/.gitignore"; fi
# *.log: real drop-copy logs hold client orders and stay out of git; the synthetic samples do not.
for line in '.venv/' '__pycache__/' '.pytest_cache/' '*.egg-info/' 'repos.local.env' \
  '*.log' '!order-tree-recon/fix-samples/*.log'; do
  grep -qxF "$line" "$DEST/.gitignore" || printf '%s\n' "$line" >>"$DEST/.gitignore"
done
echo "  updated  .gitignore"

README="README.md"
[ -e "$DEST/$README" ] && [ "$FORCE" -ne 1 ] && README="README.order-tree-recon.md"
cat >"$DEST/$README" <<'EOF'
# Order tree reconciliation (standalone)

Raptor -> algo -> order router order trees in Deephaven, searched by moniker / symbol /
side, with every level's CumQty / LeavesQty reconciled against the level below it.
Exported from `deephaven-fix42-dashboard/claude-code` by
`order-tree-recon/scripts/export_standalone.sh`. The design is in
[docs/14](docs/14-order-tree-reconciliation.md), the runbook in
[order-tree-recon/README.md](order-tree-recon/README.md).

## Run it

```bash
bash order-tree-recon/run_tests.sh                    # unit tests (Python 3.10+, creates .venv)
bash order-tree-recon/scripts/run_demo.sh             # podman compose up, mock order tables
OTR_SOURCE=fix-files bash order-tree-recon/scripts/run_demo.sh   # the sample FIX tapes
# open http://localhost:10000/iframe/widget/?name=order_tree_dashboard
bash order-tree-recon/scripts/run_demo.sh down
```

## Your own data

- **FIX drop-copy logs** -- put the files and a `fix_config.json` (start from
  `order-tree-recon/fix-samples/fix_config.json`) in one folder, then
  `OTR_SOURCE=fix-files OTR_FIX_HOST_DIR=/abs/path bash order-tree-recon/scripts/run_demo.sh`.
  Doc 14 section 12 covers the config, and live feeds (Kafka) as well.
- **Order tables already in Deephaven** -- edit `order-tree-recon/src/order_tree_recon/sources.py`
  (doc 14 section 3).
- **Your own Deephaven server** -- put `order-tree-recon/src` on its `PYTHONPATH` and run
  `from order_tree_recon import app; app.main(globals())` in the console, or call
  `fix_dag.build_from_fix(...)` / `dag.build_order_tree(...)` from your own script.
  The package needs nothing but `deephaven` and `deephaven.ui`.

## Company artifact repositories (JFrog)

Every image and the pip index come from `repos.env`. Copy `repos.local.env.example` to
`repos.local.env`, point it at your Artifactory and run `bash scripts/repos.sh check`
(doc 15).
EOF
echo "  wrote    $README"
if [ -f "$ROOT/repos.local.env" ]; then
  echo
  echo "note: $ROOT/repos.local.env (your JFrog override) was not copied; copy it to $DEST/ if you want it there."
fi
echo
echo "done. next: cd '$DEST' && bash order-tree-recon/run_tests.sh"
