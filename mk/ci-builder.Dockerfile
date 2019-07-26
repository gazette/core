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
      libzstd-dev \
      protobuf-compiler \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

ARG GOLANG_VERSION=1.12.7
ARG GOLANG_SHA256=66d83bfb5a9ede000e33c6579a91a29e6b101829ad41fffb5c5bb6c900e109d9

ARG DOCKER_VERSION=18.09.6
ARG DOCKER_SHA256=1f3f6774117765279fce64ee7f76abbb5f260264548cf80631d68fb2d795bb09

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
