#!/bin/bash
set -Euxo pipefail

# Base V2 directory which parents this script.
V2DIR="$(cd "$(dirname "$0")/.." && pwd)"

GAZETTE_BASE_VER=2.0.0

# If not already present, pull a base runtime image with compiled RocksDB
# libraries and a protobuf compiler. This image is used both for building gazette
# itself, and also for multi-stage Dockerfiles which pick out compiled binaries of
# the gazette build but still require the RocksDB runtime:
if [[ "$(docker images -q liveramp/gazette-base:${GAZETTE_BASE_VER} 2> /dev/null)" == "" ]]; then
    docker pull liveramp/gazette-base:${GAZETTE_BASE_VER};
fi

# Build and test Gazette. This image includes all Gazette source, vendored
# dependencies, compiled packages and binaries, and only completes after
# all tests pass.
docker build ${V2DIR} --file ${V2DIR}/build/Dockerfile.gazette-build --tag gazette-build

# Create the `gazette` command container, which plucks the `gazette2` and
# `gazctl` binaries onto the `gazette-base` image.
docker build ${V2DIR} --file ${V2DIR}/build/cmd/Dockerfile.gazette --tag gazette

# Gazette examples also have Dockerized build targets. Build the `stream-sum`
# example, which builds the stream-sum plugin and adds it to `gazette-base`
# with the `run-consumer` binary.
docker build ${V2DIR} --file ${V2DIR}/build/examples/Dockerfile.stream-sum --tag stream-sum

# Build the `word-count` example.
docker build ${V2DIR} --file ${V2DIR}/build/examples/Dockerfile.word-count --tag word-count
