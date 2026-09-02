# Deephaven server 42.4 + the AMPS python client — the image every server in
# docker/docker-compose.remote-uri.yml runs (docs/10-deephaven-remote-uri.md §11).
#
# Build it (context is docker/, so the compose file's `build: {context: .}` and this command
# produce the same image):
#
#   podman build -t localhost/fix42-deephaven-amps:42.4 -f docker/deephaven-amps.Dockerfile docker
#   podman compose -f docker/docker-compose.remote-uri.yml build     # the same thing, from compose
#
# Why a derived image at all — this repo has none, on purpose (docker/docker-compose.yml runs the
# stock server because deephaven.ui and deephaven.plot.express are already bundled):
#
#   * the remote-URI leaves read their FIX tapes from AMPS, and no Deephaven image ships an AMPS
#     client. It has to be installed.
#   * `podman exec <container> pip install ...` does not survive `down`, and this stack has N+1
#     servers to keep in step, so the one-off exec that is tolerable for a single-server
#     experiment turns into a startup ritual that is easy to get half-right (one leaf silently
#     without a client = one hub tape silently missing).
#
# Only the AMPS client is added. In particular pydeephaven is NOT installed: the collector reaches
# the leaves with the in-server java client behind deephaven.uri / deephaven.barrage (doc 10 §3),
# and a second gRPC stack inside the same JVM process would buy nothing. pydeephaven stays an
# e2e-only dependency, installed in the e2e's own venv on the host.
FROM ghcr.io/deephaven/server:42.4

# amps-python-client: commercial binary wheel on PyPI (manylinux x86_64/aarch64); not in the stock image.
# The image runs as root with its venv on PATH, so a plain `pip install` lands in the interpreter
# the server actually uses. The version matches the broker the demo runs against (AMPS 5.3.5.x).
RUN pip install --no-cache-dir amps-python-client==5.3.5.7
