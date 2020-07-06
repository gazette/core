# common-build.mk defines build rules which are common to both
# go.gazette.dev/core, and external consumer application projects
# which re-use the Gazette build infrastructure.


# The ci-builder-image target builds a Docker image suitable for building
# gazette. It is the primary image used by gazette continuous integration builds.
ci-builder-image: ${WORKDIR}/ci-builder-image.tar
	docker load -i ${WORKDIR}/ci-builder-image.tar

# Builds the ci-builder docker image and also saves it as a tar file in the build directory
# This allows us to skip building the image without docker needing to check each individual
# layer, and also allows the tar file to be cached
${WORKDIR}/ci-builder-image.tar:
	docker build -t gazette/ci-builder:latest - <  ${COREDIR}/mk/ci-builder.Dockerfile
	mkdir -p ${WORKDIR}
	docker save -o ${WORKDIR}/ci-builder-image.tar gazette/ci-builder:latest

# On OSX, there's magix behind the scenes to make files created by a docker container automatically
# be owned by the current user on the host. This is not so in linux, so we need to set the
# appropriate user and group ids for the container. We use the same groupid that's used in
# /var/run/docker.sock to ensure that the process in the container will be allowed to access it
host_os = $(shell uname -s)
DOCKER_USER_ARG = 
ifeq ($(host_os),Linux)
	DOCKER_USER_ARG = --user $(shell id -u):$(shell id -g) --group-add $(shell stat -c '%g' /var/run/docker.sock)
endif


STATIC =
ifdef STATIC
GOTAGS = -tags static
endif

# The as-ci rule recursively calls `make` _within_ a instance of the ci-builder-image,
# and bind-mounting the gazette repository into the container. This rule allows for
# idempotent gazette builds which exactly match those produced by the CI builder.
# It uses a few tricks to keep builds fast:
#  * The gazette repository checkout is bind-mounted into the container.
#  * A ${WORKDIR}-ci directory is bind-mounted under the containers relative work
#    directory. This keeps intermediate files of local vs "as-ci" builds separate
#    so they don't clobber one another.
#  * The GOPATH and GOCACHE variables are set to fall within the mounted work
#    directory. Notably, this means repeat invocations can re-use the go modules
#    cache, and the go build cache.
#  * The Host's Docker socket is bind-mounted into the container, which has a docker
#    client. This allows the ci-builder container to itself build Docker images.
as-ci: ci-builder-image
	mkdir -p ${WORKDIR} ${WORKDIR}-ci
	# Strip root prefix from WORKDIR to build its equivalent within the container. 
	ROOT_CI=/gazette ;\
	WORK_CI=$${ROOT_CI}$(subst ${ROOTDIR},,${WORKDIR}) ;\
	docker run $(DOCKER_USER_ARG) \
		--rm \
		--tty \
		--mount src=${WORKDIR}-ci,target=$${WORK_CI},type=bind \
		--mount src=${ROOTDIR},target=$${ROOT_CI},type=bind \
		--env  GOPATH=$${WORK_CI}/go-path \
		--env GOCACHE=$${WORK_CI}/go-build-cache \
		--mount src=/var/run/docker.sock,target=/var/run/docker.sock,type=bind \
		gazette/ci-builder /bin/sh -ec \
			"go mod download && make ${target} VERSION=${VERSION} DATE=${DATE} REGISTRY=${REGISTRY} RELEASE_TAG=${RELEASE_TAG} STATIC=$(STATIC)"


# Go build & test targets.
go-install: $(protobuf-targets)
	$(info using dynamic libs)
	MBP=go.gazette.dev/core/mainboilerplate ;\
	go install -v $(GOTAGS) \
		-ldflags '-X $${MBP}.Version=${VERSION} -X $${MBP}.BuildDate=${DATE}' ./...

go-test-fast: ${protobuf-targets}
	go test -p ${NPROC} ./...
go-test-ci: ${protobuf-targets}
	GORACE="halt_on_error=1" go test -p ${NPROC} -race -count=15 ./...

# The ci-release-% implicit rule builds a Docker image named by the rule
# stem, using binaries enumerated by a `-target` suffix. For example,
# an invocation with `ci-release-gazette-examples` has a stem `gazette-examples`, and will
# package binaries listed in `ci-release-gazette-examples-targets` into a docker
# image named `gazette/examples:latest`.
# Note that the release image does not contain dynamic libraries for rocksdb and such, so
# anything that requires those should use `STATIC=1` to link those statically
.SECONDEXPANSION:
ci-release-%: go-install $$($$@-targets)
	rm -rf ${WORKDIR}/ci-release
	mkdir -p ${WORKDIR}/ci-release
	ln ${$@-targets} ${WORKDIR}/ci-release
	docker build \
		-f ${COREDIR}/mk/ci-release.Dockerfile \
		-t $(subst -,/,$*):latest \
		${WORKDIR}/ci-release/

# Run the protobuf compiler to generate message and gRPC service implementations.
# Invoke protoc with local and third-party include paths set. The `go list` tool
# is used to map submodules to corresponding go.mod versions and paths.
%.pb.go: %.proto ${WORKDIR}/protoc-gen-gogo
	PATH=${WORKDIR}:$${PATH} ;\
	protoc -I . $(foreach module, $(PROTOC_INC_MODULES), -I$(module_path)) \
	--gogo_out=paths=source_relative,plugins=grpc:. $*.proto

# Rule to build protoc-gen-gogo.
${WORKDIR}/protoc-gen-gogo:
	go mod download github.com/golang/protobuf
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gogo

# Rule for generic go-install-able Go binaries.
${WORKDIR}/go-path/bin/%: go-install

.PHONY: ci-builder-image as-ci go-install go-test-fast go-test-ci
