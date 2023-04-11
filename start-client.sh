#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${script_dir}/nodes.sh"

${HOME}/go/bin/adfs --app client \
  --hostname ${controller} \
  --host-port ${controller_port} \
  --storage-dir /bigdata/$(whoami)/adfs/client
