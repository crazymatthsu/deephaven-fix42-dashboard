# Deephaven server 42.4 + boto3 -- the image docker/docker-compose.market-data.yml runs
# (docs/11-market-data-demo.md §11).
#
# Build it (context is docker/, so the compose file's `build: {context: .}` and this command
# produce the same image):
#
#   podman build -t localhost/fix42-deephaven-market-data:42.4 -f docker/deephaven-market-data.Dockerfile docker
#   podman compose -f docker/docker-compose.market-data.yml build     # the same thing, from compose
#
# Why a derived image: the market-data app LISTS S3 objects with boto3 (which files exist
# for a day prefix, which days exist at all) while Deephaven READS the parquet through its
# own S3 channel provider (`deephaven.experimental.s3.S3Instructions`). The stock image has
# the reader but no boto3, and `podman exec ... pip install boto3` does not survive `down`.
# For MD_SOURCE=local boto3 is simply unused: the same image serves both sources so that
# switching is one environment variable, not a rebuild.
#
# deephaven.ui and deephaven.plot.express are already bundled in the base image (see the
# README's "Deephaven image" section), so nothing else is added.
# Every external source comes from claude-code/repos.env (docs/15); compose passes these as
# build args. The defaults are the public ones, so a plain `podman build` still works.
ARG DEEPHAVEN_IMAGE=ghcr.io/deephaven/server:42.4
FROM ${DEEPHAVEN_IMAGE}
# pip reads PIP_INDEX_URL / PIP_TRUSTED_HOST from the environment; an empty trusted host is
# unset rather than handed to pip as an empty value.
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=

RUN if [ -z "$PIP_TRUSTED_HOST" ]; then unset PIP_TRUSTED_HOST; fi; \
    pip install --no-cache-dir "boto3>=1.34"
