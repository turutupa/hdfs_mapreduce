#!/usr/bin/env bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo ${script_dir}

log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Installing..."
cd ./dfs
go install || exit 1 # Exit if compile+install fails
echo "Done installing A-DFS!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/adfs \
  --app controller \
  --port ${controller_port} \
  --verbose" &> "${log_dir}/controller.log" &

sleep 1

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "Storage Node ${node}"
    ssh "${node}" "${HOME}/go/bin/adfs \
      --app storage-node \
      --port 11001 \
      --hostname ${controller} \
      --host-port ${controller_port} \
      --storage-dir /bigdata/$(whoami)/adfs/${node} \
      --plugins-dir /bigdata/$(whoami)/adfs/plugins/${node} \
      --compute-storage-dir /bigdata/$(whoami)/adfs/compute/${node} \
      --verbose" &> "${log_dir}/${node}.log" &
done

echo "Starting Compute Engine..."
ssh "${compute_engine}" "${HOME}/go/bin/adfs \
  --app compute-engine \
  --port ${compute_engine_port} \
  --hostname ${controller} \
  --host-port ${controller_port} \
  --verbose" &> "${log_dir}/compute_engine.log" &

echo "Cluser startup complete!"
