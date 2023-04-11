#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR/dfs
go run main.go \
  --app client \
  --hostname localhost \
  --host-port 6000 \
  --storage-dir $SCRIPT_DIR/data/client-downloads
