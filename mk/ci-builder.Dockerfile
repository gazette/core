# 18.04 (bionic) is latest Ubuntu LTS release. We require some of its updated
# packages (notably libzstd) over Debian stretch.
FROM ubuntu:18.04

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      build-essential \
      ca-certificates \
      curl \
      git \
      libbz2-dev \
      libjemalloc-dev \
      liblz4-dev \
      libprotobuf-dev \
      libsnappy-dev \
      libsqlite3-dev \
      libzstd-dev \
      protobuf-compiler \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

ARG GOLANG_VERSION=1.14.2
ARG GOLANG_SHA256=6272d6e940ecb71ea5636ddb5fab3933e087c1356173c61f4a803895e947ebb3

ARG DOCKER_VERSION=19.03.8
ARG DOCKER_SHA256=7f4115dc6a3c19c917f8b9664d7b51c904def1c984e082c4600097433323cf6f

ENV PATH=/usr/local/go/bin:$PATH

RUN curl -L -o /tmp/golang.tgz \
      https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
 && echo "${GOLANG_SHA256} /tmp/golang.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/golang.tgz \
      --directory /usr/local \
 && rm /tmp/golang.tgz \
 && go version

RUN curl -L -o /tmp/docker.tgz \
      https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz \
 && sha256sum /tmp/docker.tgz \
 && echo "${DOCKER_SHA256} /tmp/docker.tgz" | sha256sum -c - \
 && tar --extract \
      --file /tmp/docker.tgz \
      --strip-components 1 \
      --directory /usr/local/bin/ \
 && rm /tmp/docker.tgz \
 && docker --version

WORKDIR /gazette
