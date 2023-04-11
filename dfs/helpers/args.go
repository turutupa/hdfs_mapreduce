package helpers

import (
	"errors"
	"log"
	"os"
	"strconv"
)

const VERBOSE_FLAG = "--verbose"

// app flag and values
const APP_FLAG = "--app"

// 3 possible apps:
const CONTROLLER_APP = "controller"
const STORAGE_NODE_APP = "storage-node"
const CLIENT_APP = "client"
const COMPUTE_ENGINE_APP = "compute-engine"

// dir where data is going to be persisted
const STORAGE_NODE_DIR = "--storage-dir"
const PLUGINS_DIR = "--plugins-dir"
const COMPUTE_STORAGE_DIR = "--compute-storage-dir"

// port flag for specified app:
// * controller
// * storage node
const PORT_FLAG = "--port"

// storage node flags:
// controller addr to connect to
const CONTROLLER_HOSTNAME_FLAG = "--hostname"
const CONTROLLER_PORT_FLAG = "--host-port"
const COMPUTE_ENGINE_HOSTNAME_FLAG = "--compute-engine-hostname"
const COMPUTE_ENGINE_PORT_FLAG = "--compute-engine-port"

// Error messages
const MISSING_APP_ERROR_MSG = "Specify App you want to run with " + APP_FLAG + " <controller/storage-node/client>"
const MISSING_LOCAL_PORT_ERROR_MSG = "Specify the Controller Port with " + PORT_FLAG + " <int>"
const MISSING_CONTROLLER_HOSTNAME_FLAG_ERROR_MSG = "Specify the remote Controller Hostname with " + CONTROLLER_HOSTNAME_FLAG + " <hostname>"
const MISSING_COMPUTE_ENGINE_HOSTNAME_FLAG_ERROR_MSG = "Specify the Compute Engine Hostname with " + COMPUTE_ENGINE_HOSTNAME_FLAG + " <hostname>"
const MISSING_CONTROLLER_PORT_FLAG_ERROR_MSG = "Specify the Controller Port with " + CONTROLLER_PORT_FLAG + " <int>"
const MISSING_COMPUTE_ENGINE_PORT_FLAG_ERROR_MSG = "Specify the Compute Engine Port with " + CONTROLLER_PORT_FLAG + " <int>"
const MISSING_STORAGE_NODE_DIR_ERROR_MSG = "Specify storage folder with " + STORAGE_NODE_DIR + "</home/username/storage-folder"
const MISSING_PLUGINS_DIR_ERROR_MSG = "Specify storage folder for plugins with " + PLUGINS_DIR + "</home/username/plugins-folder"
const MISSING_COMPUTE_STORAGE_DIR_ERROR_MSG = "Specify a temp storage dir for computations with " + COMPUTE_STORAGE_DIR + "</f1/f2/temp-compute-storage-folder"

func GetApp() string {
	return argsGet(APP_FLAG, MISSING_APP_ERROR_MSG)
}

func GetLocalPort() int {
	port := argsGet(PORT_FLAG, MISSING_LOCAL_PORT_ERROR_MSG)
	if p, err := strconv.Atoi(port); err != nil {
		log.Fatalln(MISSING_LOCAL_PORT_ERROR_MSG)
		return 0
	} else {
		return p
	}
}

func GetControllerHostname() string {
	return argsGet(CONTROLLER_HOSTNAME_FLAG, MISSING_CONTROLLER_HOSTNAME_FLAG_ERROR_MSG)
}

func GetComputeEngineHostname() string {
	return argsGet(COMPUTE_ENGINE_HOSTNAME_FLAG, MISSING_COMPUTE_ENGINE_HOSTNAME_FLAG_ERROR_MSG)
}

func GetControllerPort() int {
	port := argsGet(CONTROLLER_PORT_FLAG, MISSING_CONTROLLER_PORT_FLAG_ERROR_MSG)
	if p, err := strconv.Atoi(port); err != nil {
		log.Fatalln(MISSING_LOCAL_PORT_ERROR_MSG)
		return 0
	} else {
		return p
	}
}

func GetComputeEnginePort() int {
	port := argsGet(COMPUTE_ENGINE_PORT_FLAG, MISSING_COMPUTE_ENGINE_PORT_FLAG_ERROR_MSG)
	if p, err := strconv.Atoi(port); err != nil {
		log.Fatalln(MISSING_LOCAL_PORT_ERROR_MSG)
		return 0
	} else {
		return p
	}
}

func GetStorageDir() string {
	return argsGet(STORAGE_NODE_DIR, MISSING_STORAGE_NODE_DIR_ERROR_MSG)
}

func GetPluginsDir() string {
	return argsGet(PLUGINS_DIR, MISSING_PLUGINS_DIR_ERROR_MSG)
}

func GetComputeStorageDir() string {
	return argsGet(COMPUTE_STORAGE_DIR, MISSING_COMPUTE_STORAGE_DIR_ERROR_MSG)
}

func argsGet(flag, errorMsg string) string {
	args := os.Args
	if val, err := getFlagValue(args, flag, MISSING_APP_ERROR_MSG); err != nil {
		log.Fatalln(errorMsg)
		return ""
	} else {
		return val
	}
}

func getFlagValue(args []string, flag string, errorMsg string) (string, error) {
	for i := 0; i < len(args); i++ {
		v := args[i]
		if v == flag && (i+1) < len(args) {
			return args[i+1], nil
		}
	}
	return "", errors.New(errorMsg)
}

func Contains(flag string) bool {
	args := os.Args
	for _, val := range args {
		if val == flag {
			return true
		}
	}
	return false
}
