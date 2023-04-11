package helpers

import (
	"adfs/messages"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
)

func GetAddr(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}

func GetHostname() string {
	hostname, _ := os.Hostname()
	return hostname
}

func GetMessageHandlers(storageNodes []*messages.Node) []*messages.MessageHandler {
	msgHandlers := make([]*messages.MessageHandler, len(storageNodes))
	for i, sn := range storageNodes {
		addr := GetAddr(sn.Hostname, int(sn.Port))
		msgHandler, err := messages.GetMessageHandlerFor(addr)
		if err != nil {
			msgHandlers[i] = nil
		} else {
			msgHandlers[i] = msgHandler
		}
	}
	return msgHandlers
}

func CloseMessageHandlers(messageHandlers []*messages.MessageHandler) {
	for _, msgHandler := range messageHandlers {
		if msgHandler == nil {
			continue
		}
		if err := msgHandler.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}
}
