package server

import (
	"adfs/helpers"
	"adfs/messages"
	"net"
	"strconv"
)

const TCP = "tcp"

type Server interface {
	Start(handleConnection func(messageHandler *messages.MessageHandler))
	Stop()
	GetHostname() string
	GetPort() int
}

type ServerImpl struct {
	listener *net.Listener
	hostname string
	port     int
}

func NewServerAt(port int) (Server, error) {
	hostname := helpers.GetHostname()
	addr := ":" + strconv.Itoa(port)
	if listener, err := net.Listen(TCP, addr); err != nil {
		return nil, err
	} else {
		return &ServerImpl{
			listener: &listener,
			hostname: hostname,
			port:     port,
		}, nil
	}
}

func (s *ServerImpl) Start(handleConnection func(msgHandler *messages.MessageHandler)) {
	listener := *s.listener

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleConnection(msgHandler)
		}
	}
}

func (s *ServerImpl) Stop() {}

func (s *ServerImpl) GetHostname() string {
	return s.hostname
}

func (s *ServerImpl) GetPort() int {
	return s.port
}
