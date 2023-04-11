#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${script_dir}/nodes.sh"

echo "Stopping Controller..."
ssh "${controller}" "pkill -u "$(whoami)" adfs"

echo "Stopping Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "pkill -u "$(whoami)" adfs"
done

echo "Stopping Compute Engine..."
ssh "${compute_engine}" "pkill -u "$(whoami)" adfs"

echo "Done!"
