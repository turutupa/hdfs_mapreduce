package main

import (
	"adfs/client"
	"adfs/compute_engine"
	"adfs/controller"
	h "adfs/helpers"
	"adfs/storageNode"
	"fmt"
	"os"
)

func main() {
	app := h.GetApp()

	h.ClearTerminal()
	switch app {
	case h.CONTROLLER_APP:
		h.PrintTitle("CONTROLLER")
		controller.Init(controller.Config{Port: h.GetLocalPort()})
		return
	case h.COMPUTE_ENGINE_APP:
		h.PrintTitle("COMPUTE ENGINE")
		compute_engine.Init(compute_engine.Config{
			Port:               h.GetLocalPort(),
			ControllerHostname: h.GetControllerHostname(),
			ControllerPort:     h.GetControllerPort(),
		})
		return
	case h.STORAGE_NODE_APP:
		h.PrintTitle("S.NODE")
		config := storageNode.Config{
			Port:               h.GetLocalPort(),
			ControllerHostname: h.GetControllerHostname(),
			ControllerPort:     h.GetControllerPort(),
			StorageDir:         h.GetStorageDir(),
			PluginsDir:         h.GetPluginsDir(),
			ComputeStorageDir:  h.GetComputeStorageDir(),
		}
		storageNode.Init(config)
		return
	case h.CLIENT_APP:
		config := client.Config{
			HomeDir:        h.GetHomeDir(),
			ControllerHost: h.GetControllerHostname(),
			ControllerPort: h.GetControllerPort(),
			StorageDir:     h.GetStorageDir(),
		}
		client.Init(config)
		return
	}
	fmt.Println(h.MISSING_APP_ERROR_MSG)
	os.Exit(1)
}
