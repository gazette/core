#!/usr/bin/env bash
set -Eeu -o pipefail

# Base V2 directory which parents this script.
V2DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Build and test Gazette. This image includes all Gazette source, vendored
# dependencies, compiled packages and binaries, and only completes after
# all tests pass.
docker build ${V2DIR} \
    --file ${V2DIR}/build/Dockerfile \
    --target build \
    --tag liveramp/gazette-build:latest \
    --cache-from liveramp/gazette-build:latest

# Create the `gazette` image, which plucks the `gazette`, `gazctl` and
# `run-consumer` onto a base runtime image.
docker build ${V2DIR} \
    --file ${V2DIR}/build/Dockerfile \
    --target gazette \
    --tag liveramp/gazette:latest

# Create the `gazette-examples` image, which further plucks `stream-sum` and
# `word-count` example artifacts onto the `gazette` image.
docker build ${V2DIR} \
    --file ${V2DIR}/build/Dockerfile \
    --target examples \
    --tag liveramp/gazette-examples:latest
