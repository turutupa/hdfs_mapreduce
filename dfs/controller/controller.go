package controller

import (
	"adfs/common"
	"adfs/helpers"
	m "adfs/messages"
	s "adfs/server"

	"strings"

	"github.com/sirupsen/logrus"
)

type Controller interface {
	Start()
	Stop()
}

type ControllerImpl struct {
	port              int
	server            s.Server
	zookeeper         Zookeeper
	fileIndex         FileIndex
	computeEngineAddr string
}

type ControllerConfig struct {
	Zookeeper
	FileIndex
	s.Server
	computeEngineAddr string
}

func NewController(config ControllerConfig) Controller {
	return &ControllerImpl{
		zookeeper: config.Zookeeper,
		fileIndex: config.FileIndex,
		server:    config.Server,
	}
}

func (c *ControllerImpl) Start() {
	c.zookeeper.AddListenerOnNodeDown(c.fileIndex.NodeDown)
	c.zookeeper.Start()
	logrus.Info("Zookeeper running")
	c.fileIndex.Start()
	logrus.Info("File Index running")
	logrus.WithFields(logrus.Fields{
		"PORT": c.server.GetPort(),
	}).Info("Controller listening")
	c.server.Start(c.handleConnection)
}

func (c *ControllerImpl) Stop() {
	c.zookeeper.Stop()
	c.fileIndex.Stop()
	c.server.Stop()
}

func (c *ControllerImpl) handleConnection(messageHandler *m.MessageHandler) {
	wrapper, _ := messageHandler.Receive()
	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_RegistrationMessage:
		c.handleRegistration(msg.RegistrationMessage.Node)
	case *m.Wrapper_HeartbeatMessage:
		c.handleHeartbeat(msg.HeartbeatMessage)
		c.sendOnlineStorageNodes(messageHandler)
	case *m.Wrapper_ActionRequestMessage:
		c.handleActionRequest(messageHandler, msg.ActionRequestMessage)
	case nil:
		logrus.WithFields(logrus.Fields{
			"remoteAddr": messageHandler.GetRemoteAddr(),
		}).Info("Closed connection")
	default:
		logrus.Error("Unexpected message type")
	}
	c.handleCloseConnection(messageHandler)
}

func (c *ControllerImpl) handleRegistration(node *m.Node) {
	if node.Uuid == common.COMPUTE_ENGINE {
		c.computeEngineAddr = helpers.GetAddr(node.Hostname, int(node.Port))
		logrus.WithFields(logrus.Fields{
			"computeEngineAddr": strings.TrimSpace(c.computeEngineAddr),
		}).Info("Compute Engine Registration")
		return
	}
	c.zookeeper.RegisterNode(node)
}

func (c *ControllerImpl) handleHeartbeat(heartbeat *m.Heartbeat) {
	chunks := heartbeat.Chunks
	uuid := heartbeat.StorageNode.Uuid
	c.zookeeper.Heartbeat(uuid, heartbeat.Stats)
	c.fileIndex.PutAll(heartbeat.StorageNode, chunks)
}

func (c *ControllerImpl) sendOnlineStorageNodes(messageHandler *m.MessageHandler) {
	nodes := make([]*m.Node, 0)
	for _, n := range c.zookeeper.GetNodes() {
		nodes = append(nodes, &m.Node{
			Uuid:     n.Uuid,
			Hostname: n.Hostname,
			Port:     int32(n.Port),
			Stats:    n.Stats,
		})
	}
	err := messageHandler.SendNodes(nodes)
	if err != nil {
		logrus.Error("Something went wrong sending back Online Storage Nodes to Heartbeat response")
	}
}

func (c *ControllerImpl) handleActionRequest(
	messageHandler *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	fields := logrus.Fields{
		"Type":      actionRequest.Type,
		"Filename":  actionRequest.FileName,
		"ChunkName": actionRequest.ChunkName,
	}
	if actionRequest.Plugin != nil {
		fields["PluginName"] = actionRequest.Plugin.Name
		fields["hasPlugin"] = len(actionRequest.Plugin.Plugin) > 0
	}
	logrus.WithFields(fields).Info("Received Action Request")
	switch *actionRequest.Type.Enum() {
	case m.ActionType_LS:
		c.handleLS(messageHandler)
	case m.ActionType_GET:
		c.handleGET(messageHandler, actionRequest)
	case m.ActionType_PUT:
		c.handlePUT(messageHandler, actionRequest)
	case m.ActionType_RM:
		c.handleRM(messageHandler, actionRequest)
	case m.ActionType_COMPUTE:
		c.handleCompute(messageHandler, actionRequest)
	case m.ActionType_CLUSTER_STATS:
		c.handleClusterStats(messageHandler)
	}
}

func (c *ControllerImpl) handleLS(messageHandler *m.MessageHandler) {
	filesMetadata := c.fileIndex.Ls()
	var fileIndex []*m.File
	for _, file := range filesMetadata {
		var chunks []*m.Chunk
		for _, chunk := range file.chunks {
			chunks = append(chunks, chunk)
		}

		fileIndex = append(fileIndex, &m.File{
			Dirname: file.filename,
			Chunks:  chunks,
		})
	}
	messageHandler.SendFilesMetadata(fileIndex)
}

func (c *ControllerImpl) handleGET(
	messageHandler *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	filename := actionRequest.FileName
	file, err := c.fileIndex.Get(filename)
	if err != nil {
		messageHandler.SendFailAck(err.Error())
	} else {
		messageHandler.SendFileMetadata(file)
	}
}

func (c *ControllerImpl) handlePUT(
	messageHandler *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	filename := actionRequest.FileName
	nodes := c.zookeeper.GetNodes()
	if len(nodes) == 0 {
		errorMsg := "Currently there are not Storage Nodes online"
		messageHandler.SendFailAck(errorMsg)
	} else if c.fileIndex.FileExists(filename) {
		errorMsg := "FileName already exists. Please choose a different name."
		messageHandler.SendFailAck(errorMsg)
	} else {
		c.fileIndex.ReserveSlot(filename)
		// TODO (TLDR): send only a small number of available Storage Nodes using better algo
		// rn it sends all nodes as available for uploading files.
		// Meaning client will upload a chunk to each of the nodes in
		// the system.
		n := make([]*m.Node, 0)
		for _, node := range nodes {
			n = append(n, &m.Node{
				Hostname: node.Hostname,
				Port:     int32(node.Port),
				Uuid:     node.Uuid,
				Stats:    node.Stats,
			})
		}
		messageHandler.SendNodes(n)
	}
}

func (c *ControllerImpl) handleRM(
	messageHandler *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	filename := actionRequest.FileName
	file, err := c.fileIndex.Get(filename)
	if err != nil {
		messageHandler.SendFailAck(err.Error())
		return
	}
	c.fileIndex.Rm(filename)
	go func() {
		for _, chunk := range file.Chunks {
			for _, sn := range chunk.StorageNodes {
				chunkName := chunk.ChunkName
				addr := helpers.GetAddr(sn.Hostname, int(sn.Port))
				msgHandler, _ := m.GetMessageHandlerFor(addr)
				msgHandler.SendChunkRemoveRequest(chunkName)
				msgHandler.Close()
			}
		}
	}()
	messageHandler.SendSuccessAck()
}

func (c *ControllerImpl) handleCompute(
	clientConn *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	computeEngineConn, err := m.GetMessageHandlerFor(c.computeEngineAddr)
	defer clientConn.Close()
	defer computeEngineConn.Close()
	if err != nil {
		clientConn.SendFailAck("Compute Engine is OFFLINE")
		return
	}
	targetFilename := actionRequest.FileName
	plugin := actionRequest.Plugin
	outputFilename := actionRequest.OutputFilename
	computeEngineConn.SendComputeRequest(
		targetFilename,
		plugin.Name,
		outputFilename,
		plugin.Plugin,
		m.ComputeType_MAP,
		nil,
	)
	for { // we just going to redirect messages from ComputeEngine to Client
		wrapper, _ := computeEngineConn.Receive()
		switch msg := wrapper.Msg.(type) {
		case *m.Wrapper_ComputationStatusMessage:
			clientConn.Redirect(wrapper)
			if !msg.ComputationStatusMessage.Ok ||
				msg.ComputationStatusMessage.Status == m.JobStatus_job_done {
				return
			}
		default:
			logrus.WithFields(logrus.Fields{
				"Error": "Unknown message type or conn closed",
			}).Error("Something went wrong")
			return
		}
		// wrapper, _ = computeEngineConn.Receive()
		// send success ack when finished
		// clientConn.SendSuccessAck()
	}
}

func (c *ControllerImpl) handleClusterStats(messageHandler *m.MessageHandler) {
	var storageNodes []*m.Node
	znodes := c.zookeeper.GetNodes()
	for _, zn := range znodes {
		storageNodes = append(storageNodes, &m.Node{
			Uuid:     zn.Uuid,
			Hostname: zn.Hostname,
			Port:     int32(zn.Port),
			Stats:    zn.Stats,
		})
	}

	messageHandler.SendNodes(storageNodes)
}

func (c *ControllerImpl) handleCloseConnection(messageHandler *m.MessageHandler) {
	err := messageHandler.Close()
	if err != nil {
		logrus.Error(err.Error())
	}
}
