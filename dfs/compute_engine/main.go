package compute_engine

import (
	"adfs/helpers"
	s "adfs/server"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Port               int
	ControllerHostname string
	ControllerPort     int
}

func Init(config Config) {
	controllerAddr := helpers.GetAddr(config.ControllerHostname, config.ControllerPort)
	server, err := s.NewServerAt(config.Port)
	if err != nil {
		logrus.Error("Error creating local server for Compute Engine: " + err.Error())
	}
	computeEngine := NewComputeEngineResourceManager(controllerAddr, server)
	computeEngine.Start()
}
