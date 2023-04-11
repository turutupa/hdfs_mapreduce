package storageNode

import (
	"adfs/helpers"
	s "adfs/server"
)

type Config struct {
	Port               int
	ControllerHostname string
	ControllerPort     int
	StorageDir         string
	PluginsDir         string
	ComputeStorageDir  string
}

func Init(config Config) {
	uuid, uuidErr := helpers.NewUUID()
	controllerAddr := helpers.GetAddr(config.ControllerHostname, config.ControllerPort)
	server, serverErr := s.NewServerAt(config.Port)
	storageIO := NewStorageIO()
	statsBoard := NewStatsBoard()
	if serverErr != nil {
		panic(serverErr)
	}
	if uuidErr != nil {
		panic(uuidErr)
	}
	storageNode := NewStorageNode(
		uuid,
		controllerAddr,
		server,
		storageIO,
		statsBoard,
		config.StorageDir,
		config.PluginsDir,
		config.ComputeStorageDir,
	)
	storageNode.Start()
}
