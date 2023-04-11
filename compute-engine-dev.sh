#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/dfs
go run main.go \
  --app "compute-engine" \
  --port 6001 \
  --hostname localhost \
  --host-port 6000 
