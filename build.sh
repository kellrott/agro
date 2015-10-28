#!/bin/bash

cd `dirname $0`
export GOPATH=`pwd`
go get -u github.com/golang/protobuf/protoc-gen-go
export PATH=$PATH:`pwd`/bin
protoc --go_out=plugins=grpc:src/agro/ proto/agro.proto 
protoc --python_out pyagro -I proto  --grpc_out=pyagro --plugin=protoc-gen-grpc=`which grpc_python_plugin`  proto/agro.proto 
