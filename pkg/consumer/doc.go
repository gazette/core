// Package consumer is a client library for distributed, stateful topic consumption.

//go:generate protoc -I . -I ../../vendor --gogo_out=plugins=grpc:. service.proto
package consumer
