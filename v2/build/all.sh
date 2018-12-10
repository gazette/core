#!/usr/bin/env bash
set -Eeux -o pipefail

# Root directory of repository.
readonly ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

readonly VENDOR_IMAGE="rupertchen/gazette-vendor:latest"
if [[ ${CIRCLECI:-} = true ]]; then
    docker pull "$VENDOR_IMAGE"

    readonly CF_BUILD="--cache-from $VENDOR_IMAGE"
    readonly CF_GAZETTE="--cache-from liveramp/gazette-build:latest"
    readonly CF_EXAMPLES="--cache-from liveramp/gazette-build:latest --cache-from liveramp/gazette:latest"

    if [[ $CIRCLE_BRANCH = "master" ]]; then
        SHOULD_PUSH_VENDOR=true
    fi
fi

# Build and test Gazette. This image includes all Gazette source, vendored
# dependencies, compiled packages and binaries, and only completes after
# all tests pass.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target build \
    --tag liveramp/gazette-build:latest \
    ${CF_BUILD:-}

# Create the `gazette` image, which plucks the `gazette`, `gazctl` and
# `run-consumer` onto a base runtime image.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target gazette \
    --tag liveramp/gazette:latest \
    ${CF_GAZETTE:-}

# Create the `gazette-examples` image, which further plucks `stream-sum` and
# `word-count` example artifacts onto the `gazette` image.
docker build ${ROOT} \
    --file ${ROOT}/v2/build/Dockerfile \
    --target examples \
    --tag liveramp/gazette-examples:latest \
    ${CF_EXAMPLES:-}

# Publish vendor image which is used as a cache in CI builds.
if [[ ${SHOULD_PUSH_VENDOR:-} = true && -n "${DOCKER_USER:-}" && -n "${DOCKER_PASS:-}" ]]; then
    # Temporarily disable xtrace to hide password ($DOCKER_PASS).
    set +x
    echo "Log in to Docker as $DOCKER_USER"
    echo "$DOCKER_PASS" | docker login -u $DOCKER_USER --password-stdin
    set -x

    docker build . \
      --file ./v2/build/Dockerfile \
      --target vendor \
      --tag "$VENDOR_IMAGE" \
      --cache-from "$VENDOR_IMAGE"
    docker push "$VENDOR_IMAGE"
fi
