package controller

import (
	s "adfs/server"
)

type Config struct {
	Port int
}

func Init(config Config) {
	server, err := s.NewServerAt(config.Port)
	if err != nil {
		panic(err)
	}
	fileIndex := NewFileIndex()
	zookeeper := NewZookeeper()
	controller := NewController(ControllerConfig{
		Server:    server,
		Zookeeper: zookeeper,
		FileIndex: fileIndex,
	})
	controller.Start()
}
