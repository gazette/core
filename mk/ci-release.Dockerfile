FROM ubuntu:18.04

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      libjemalloc1 \
 && rm -rf /var/lib/apt/lists/*

# Run as non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette

# Copy binaries to the image. It's expected that rocksdb, if used at all, will be statically linked
COPY * /usr/local/bin/


