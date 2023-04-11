#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/dfs
go run main.go \
  --app "storage-node" \
  --port $2 \
  --hostname localhost \
  --host-port 6000 \
  --storage-dir "$SCRIPT_DIR/data/$1" \
  --plugins-dir "$SCRIPT_DIR/plugins/$1" \
  --compute-storage-dir "$SCRIPT_DIR/compute/$1"

