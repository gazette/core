# common-config.mk defines build configuration which is common to both
# go.gazette.dev/core, and external consumer application projects
# which re-use the Gazette build infrastructure.

# Git version & date which are injected into built binaries.
VERSION = $(shell git describe --dirty --tags)
DATE    = $(shell date +%F-%T-%Z)
# Number of available processors for parallel builds.
NPROC := $(if ${NPROC},${NPROC},$(shell nproc))

# Repository root (the directory of the invoked Makefile).
ROOTDIR  = $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
# Location of the gazette/core repository (one level up from common-config.mk)
COREDIR  = $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
# Location to place intermediate files and output artifacts
# during the build process. Note the go tool ignores directories
# with leading '.' or '_'.
WORKDIR  = ${ROOTDIR}/.build

# PROTOC_INC_MODULES is an append-able list of Go modules
# which should be included with `protoc` invocations.
PROTOC_INC_MODULES += "github.com/golang/protobuf"
PROTOC_INC_MODULES += "github.com/gogo/protobuf"

# module_path expands a $(module), like go.gazette.dev/core, to the local path
# of its respository as currently specified by go.mod.
module_path = $(shell go list -f '{{ .Dir }}' -m $(module))

# These linker args really ought to be set by cgo comments in the relevant files, but this is
# currently broken in gorocksdb starting with this pr: https://github.com/tecbot/gorocksdb/pull/174
# See this issue for a better explanation: https://github.com/tecbot/gorocksdb/issues/197
# Note that STATIC is enabled by default here, since that is what we use for the consumer examples.
# If you wish to disable it, pass `STATIC=` as an argument to make.
STATIC=1
ifdef STATIC
	export CGO_LDFLAGS     = -l:librocksdb.a -l:libz.a -l:libbz2.a -l:libsnappy.a -l:liblz4.a -l:libzstd.a -lm -ldl
else
	export CGO_LDFLAGS     = -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -lm -ldl
endif
