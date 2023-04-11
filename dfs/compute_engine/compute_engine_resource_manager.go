package compute_engine

import (
	"adfs/common"
	"adfs/helpers"
	"adfs/messages"
	"adfs/server"
	"math/rand"
	"strconv"

	"github.com/sirupsen/logrus"
)

type ComputeEngineResourceManager interface {
	Start()
	Stop()
}

// CERM == ComputeEngineResourceManager
// The longer the name the more PRO you are
type CERMImpl struct {
	controllerAddr string
	server         server.Server
}

func NewComputeEngineResourceManager(controllerAddr string, server server.Server) ComputeEngineResourceManager {
	return &CERMImpl{
		controllerAddr: controllerAddr,
		server:         server,
	}
}

func (cerm *CERMImpl) Start() {
	logrus.Info("Registering to Controller")
	cerm.sendRegistrationMessage()
	cerm.server.Start(cerm.handleConnection)
}

func (cerm *CERMImpl) Stop() {}

func (cerm *CERMImpl) sendRegistrationMessage() {
	msgHandler, err := messages.GetMessageHandlerFor(cerm.controllerAddr)
	if err != nil {
		logrus.Error("A-DFS Controller OFFLINE")
		return
	}
	msgHandler.SendRegistrationMessage(
		common.COMPUTE_ENGINE,
		cerm.server.GetHostname(),
		cerm.server.GetPort(),
	)
}

func (cerm *CERMImpl) handleConnection(messageHandler *messages.MessageHandler) {
	wrapper, _ := messageHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *messages.Wrapper_ActionRequestMessage:
		filename := msg.ActionRequestMessage.FileName
		plugin := msg.ActionRequestMessage.Plugin
		outputFilename := msg.ActionRequestMessage.OutputFilename
		logrus.WithFields(logrus.Fields{
			"Filename":       filename,
			"PluginName":     plugin.Name,
			"OutputFilename": outputFilename,
			"hasPlugin":      len(plugin.Plugin) > 0,
		}).Info("New Compute Request")
		cerm.handleNewJob(messageHandler, filename, plugin, outputFilename)
	}
}

func (cerm *CERMImpl) handleNewJob(
	statusUpdateConn *messages.MessageHandler,
	filename string,
	plugin *messages.Plugin,
	outputFilename string,
) {
	defer statusUpdateConn.Close()
	controllerConn, err := messages.GetMessageHandlerFor(cerm.controllerAddr)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ControllerAddr": cerm.controllerAddr}).Error("Could not connect to controller")
		return
	}

	/** Get target file metadata in order to contacat each node to start compute job */
	controllerConn.SendGETRequest(filename) // retrieve target file information
	wrapper, _ := controllerConn.Receive()
	controllerConn.Close()
	switch msg := wrapper.Msg.(type) {
	case *messages.Wrapper_FileMessage:
		statusUpdateConn.SendComputationStatus(messages.JobStatus_job_accepted, true, "")
		file := msg.FileMessage
		logrus.WithFields(logrus.Fields{
			"Filename":  file.Name,
			"Dirname":   file.Dirname,
			"numChunks": len(file.Chunks),
		}).Info("Target file information")
		/** Reducers assignment */
		reducers := getReducers(file.Chunks)
		logrus.WithFields(logrus.Fields{"Reducers": reducers}).Info("Auto-assigned reducers")
		/** Send computation jobs */
		filesTableChan := make(chan map[string]*messages.Node)
		for _, chunk := range msg.FileMessage.Chunks {
			go cerm.sendComputationJob(statusUpdateConn, chunk, plugin, outputFilename, reducers, filesTableChan)
		}
		reducersMap := make(map[string]*messages.Node)
		filesTable := make(map[string][]string)
		for i := 0; i < len(msg.FileMessage.Chunks); i++ {
			mapperOutputFilesTable := <-filesTableChan
			if mapperOutputFilesTable == nil {
				logrus.WithFields(logrus.Fields{"ErrorMsg": "no files table"}).Error("Mapper phase failed")
				statusUpdateConn.SendComputationStatus(
					messages.JobStatus_job_mappers,
					false,
					"Something went wrong in one of the mappers")
				return
			}
			for filename, node := range mapperOutputFilesTable {
				reducersMap[node.Uuid] = node
				if filenames, ok := filesTable[node.Uuid]; ok {
					filesTable[node.Uuid] = append(filenames, filename)
				} else {
					filesTable[node.Uuid] = []string{filename}
				}
			}
			statusMsg := "Mappers completed " + strconv.Itoa(i+1) + "/" + strconv.Itoa(len(msg.FileMessage.Chunks))
			statusUpdateConn.SendComputationStatus(
				messages.JobStatus_job_mappers,
				true,
				statusMsg)
			logrus.Info(statusMsg)
		}

		/** Get files table with the form map[Node] []string */
		reducerMap := make(map[*messages.Node][]string)
		for uuid, filenames := range filesTable {
			reducerMap[reducersMap[uuid]] = filenames
		}
		for node, filenames := range reducerMap {
			logrus.WithFields(logrus.Fields{"UUID": node.Uuid, "Files": filenames}).Info("Partitions")
		}
		statusUpdateConn.SendComputationStatus(messages.JobStatus_job_reducers, true, "Initiating Reduce phase")

		reducerChannel := make(chan bool)
		for reducerNumber, reducer := range reducers {
			go cerm.sendReduceJob(reducer, statusUpdateConn, filesTable[reducer.Uuid], plugin, reducerChannel, int32(reducerNumber), outputFilename)
		}

		reducersFailed := 0
		for i := 0; i < len(reducers); i++ {
			ok := <-reducerChannel
			if ok {
				statusUpdateConn.SendComputationStatus(
					messages.JobStatus_job_reducers,
					true,
					"Reducers completed "+strconv.Itoa(i+1)+"/"+strconv.Itoa(len(msg.FileMessage.Chunks)))
			} else {
				reducersFailed++
			}
		}
		statusUpdateConn.SendComputationStatus(
			messages.JobStatus_job_reducers,
			true,
			"Reducers successful "+strconv.Itoa(len(reducers)-reducersFailed)+
				"/"+strconv.Itoa(len(reducers))+
				"; Failed "+strconv.Itoa(reducersFailed)+"/"+strconv.Itoa(len(reducers)))

		logrus.Info("Reduce Phase complete")

		statusUpdateConn.SendComputationStatus(messages.JobStatus_job_done, true, "")

	/** Something went wrong */
	case *messages.Wrapper_AckMessage:
		logrus.WithFields(logrus.Fields{
			"ErrorMsg": msg.AckMessage.ErrorMessage,
		}).Error("Something went wrong")
		return
	}
}

func getReducers(chunks []*messages.Chunk) []*messages.Node {
	storageNodes := make(map[string]*messages.Node)
	for _, chunk := range chunks {
		for uuid, storageNode := range chunk.StorageNodes {
			storageNodes[uuid] = storageNode
		}
	}
	var reducers []*messages.Node
	// we gonna set the max amount of reducers to 3 and min 1
	var numReducers int
	if len(storageNodes) > 5 {
		numReducers = 2
	} else {
		numReducers = 1
	}
	i := 0
	for _, storageNode := range storageNodes {
		if i == numReducers {
			break
		}
		reducers = append(reducers, storageNode)
		i++
	}
	return reducers
}

func getRandomNode(chunk *messages.Chunk) *messages.Node {
	keys := make([]string, len(chunk.StorageNodes))
	i := 0
	for k := range chunk.StorageNodes {
		keys[i] = k
		i++
	}
	randomUuid := keys[rand.Intn(len(keys))]
	return chunk.StorageNodes[randomUuid]
}

func (cerm *CERMImpl) sendComputationJob(
	statusUpdateConn *messages.MessageHandler,
	chunk *messages.Chunk,
	plugin *messages.Plugin,
	outputFilename string,
	reducers []*messages.Node,
	ok chan<- map[string]*messages.Node,
) {
	if statusUpdateConn.IsClosed {
		ok <- nil
		return
	}
	sn := getRandomNode(chunk)
	snConn, err := messages.GetMessageHandlerFor(helpers.GetAddr(sn.GetHostname(), int(sn.GetPort())))
	defer snConn.Close()
	logrus.WithFields(logrus.Fields{
		"StorageNodeAddr": snConn.GetRemoteAddr(),
		"Chunk":           chunk.ChunkName,
		"Plugin":          plugin.Name,
	}).Info("Sending job to mapper")
	if err != nil {
		statusUpdateConn.SendComputationStatus(
			messages.JobStatus_job_mappers,
			false,
			"Could not connect to Mapper")
		ok <- nil
		return
	}

	/**
	* Init map phase
	* Send enough information so that mappers can map the data and transfer
	* the output to the corresponding reducers
	 */
	snConn.SendComputeRequest(
		chunk.ChunkName,
		plugin.Name,
		outputFilename,
		plugin.Plugin,
		messages.ComputeType_MAP,
		reducers,
	)
	res, _ := snConn.Receive()
	/** we need to know the names of the output files of the mappers */
	switch res := res.Msg.(type) {
	case *messages.Wrapper_ComputationStatusMessage:
		if res.ComputationStatusMessage.Ok {
			statusUpdateConn.SendComputationStatus(messages.JobStatus_job_mappers, true, res.ComputationStatusMessage.ErrorMessage)
			filesTable := res.ComputationStatusMessage.FilesTable
			ok <- filesTable
		} else {
			logrus.WithFields(logrus.Fields{
				"ErrorMsg": res.ComputationStatusMessage.ErrorMessage,
				"Stage":    res.ComputationStatusMessage.Status,
			}).Error("Error in Distributed Computation")
			ok <- nil
		}
	}
}

func (cerm *CERMImpl) sendReduceJob(
	sn *messages.Node,
	statusUpdateConn *messages.MessageHandler,
	filenames []string,
	plugin *messages.Plugin,
	ok chan<- bool,
	reducerNumber int32,
	outputFilename string,
) {
	if statusUpdateConn.IsClosed {
		return
	}

	snConn, err := messages.GetMessageHandlerFor(helpers.GetAddr(sn.GetHostname(), int(sn.GetPort())))
	defer snConn.Close()
	logrus.WithFields(logrus.Fields{
		"StorageNodeAddr": snConn.GetRemoteAddr(),
		"Plugin":          plugin.Name,
	}).Info("Sending reduce job")
	if err != nil {
		statusUpdateConn.SendComputationStatus(
			messages.JobStatus_job_reducers,
			false,
			"Could not connect to Reducer")
		ok <- false
		return
	}

	/** Init reduce phase */
	snConn.SendReduceRequest(filenames, plugin.Name, plugin.Plugin, reducerNumber, outputFilename)
	res, _ := snConn.Receive()
	/** Wait for status information */
	switch res := res.Msg.(type) {
	case *messages.Wrapper_ComputationStatusMessage:
		if res.ComputationStatusMessage.Ok {
			statusUpdateConn.SendComputationStatus(messages.JobStatus_job_reducers, true, "")
			ok <- true
		} else {
			logrus.WithFields(logrus.Fields{
				"ErrorMsg": res.ComputationStatusMessage.ErrorMessage,
				"Stage":    res.ComputationStatusMessage.Status,
			}).Error("Error in Distributed Computation")
			ok <- false
		}
	}
}
