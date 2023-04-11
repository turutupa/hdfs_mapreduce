package storageNode

import (
	c "adfs/client"
	"adfs/compute_engine"
	"adfs/helpers"
	m "adfs/messages"
	s "adfs/server"
	"bufio"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const HEARTBEAT_DELAY_S = 5
const CHUNK_REPLICAS = 2

type StorageNode interface {
	Start()
	Stop()
	Register()
	Heartbeats()
	ClusterStats() *m.Stats
}

type StorageNodeImpl struct {
	uuid               string
	controllerAddr     string
	server             s.Server
	storageIO          StorageIO
	statsBoard         StatsBoard
	replicas           map[string]*m.Node // membership table
	heartbeatScheduler *time.Ticker
	heartbeatStatusCh  chan bool
	replicasCh         chan []*m.Node
	storageDir         string
	pluginsDir         string
	computeStorageDir  string
}

func NewStorageNode(
	uuid string,
	controllerAddr string,
	server s.Server,
	storageIO StorageIO,
	statsBoard StatsBoard,
	storageDir string,
	pluginsDir string,
	computeStorageDir string,
) StorageNode {
	return &StorageNodeImpl{
		uuid:              uuid,
		controllerAddr:    controllerAddr,
		server:            server,
		storageIO:         storageIO,
		statsBoard:        statsBoard,
		replicas:          make(map[string]*m.Node, 0),
		replicasCh:        make(chan []*m.Node),
		storageDir:        storageDir,
		pluginsDir:        pluginsDir,
		computeStorageDir: computeStorageDir,
	}
}

func (sn *StorageNodeImpl) Start() {
	logrus.WithFields(logrus.Fields{"DFS Storage": sn.storageDir, "Plugins Storage": sn.pluginsDir, "Compute Storage": sn.computeStorageDir}).Info("Storage DIRS")
	sn.Register()
	sn.Heartbeats()
	go sn.worker()
	sn.statsBoard.Start()
	logrus.WithFields(logrus.Fields{"PORT": sn.server.GetPort(), "UUID": sn.uuid}).Info("Storage Node Started")
	sn.server.Start(sn.handleConnection)
}

func (sn *StorageNodeImpl) Stop() {
	sn.stopHeartbeats()
}

func (sn *StorageNodeImpl) Register() {
	logrus.WithFields(logrus.Fields{"controllerAddr": sn.controllerAddr}).Info("Sending registration message")
	msgHandler, err := m.GetMessageHandlerFor(sn.controllerAddr)
	defer msgHandler.Close()
	if err != nil {
		logrus.WithFields(logrus.Fields{"error": err.Error()}).Error("Could not connect to Controller. Reason:")
		os.Exit(1)
	}
	hostname := sn.server.GetHostname()
	port := sn.server.GetPort()
	if e := msgHandler.SendRegistrationMessage(sn.uuid, hostname, port); e != nil {
		logrus.Error("Could not register to Controller")
		os.Exit(1)
	}
	logrus.Info("Registered successfully to Controller")
}

func (sn *StorageNodeImpl) handleConnection(msgHandler *m.MessageHandler) {
	for {
		wrapper, _ := msgHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *m.Wrapper_ActionRequestMessage:
			actionRequest := msg.ActionRequestMessage
			filename := actionRequest.FileName
			chunk := actionRequest.Chunk
			var chunkName string
			if chunk != nil {
				chunkName = chunk.ChunkName
			} else {
				chunkName = actionRequest.ChunkName
			}
			computeType := actionRequest.ComputeType
			plugin := actionRequest.Plugin
			logrus.WithFields(logrus.Fields{
				"Type":      actionRequest.Type,
				"ChunkName": chunkName,
				"Filename":  filename,
			}).Info("New Request")
			switch *actionRequest.Type.Enum() {
			case m.ActionType_GET:
				sn.handleGetRequest(msgHandler, chunkName)
			case m.ActionType_RM:
				sn.handleRemoveRequest(chunkName)
			case m.ActionType_PUT:
				sn.handlePutRequest(chunk)
			case m.ActionType_COMPUTE:
				if computeType == m.ComputeType_MAP {
					go sn.handleMapRequest(msgHandler, actionRequest)
				} else if computeType == m.ComputeType_REDUCE {
					filenames := actionRequest.FileNames
					reducerNumber := actionRequest.ReducerNumber
					outputFilename := actionRequest.OutputFilename
					go sn.handleReduceRequest(msgHandler, filenames, plugin, reducerNumber, outputFilename)
				} else {
					logrus.Error("Invalid compute type")
					msgHandler.Close()
					return
				}
			case m.ActionType_COMPUTE_STORE:
				filename := actionRequest.FileName
				data := actionRequest.Data
				go sn.storeMapperOutput(filename, data)
			}
		case nil:
			msgHandler.Close()
			return
		default:
			msgHandler.Close()
			return
		}
	}
}

func (sn *StorageNodeImpl) handleGetRequest(messageHandler *m.MessageHandler, chunkName string) {
	file, err := sn.storageIO.Retrieve(sn.storageDir + chunkName)
	if err != nil {
		messageHandler.SendFailAck(err.Error())
		return
	}
	chunk := &m.Chunk{}
	err = proto.Unmarshal(file, chunk)
	if err != nil {
		messageHandler.SendFailAck(err.Error())
	} else {
		messageHandler.SendChunk(chunk)
		sn.statsBoard.AddDownloaded()
	}
}

func (sn *StorageNodeImpl) handleRemoveRequest(chunkName string) {
	sn.storageIO.Delete(sn.storageDir + chunkName)
}

func (sn *StorageNodeImpl) handlePutRequest(chunk *m.Chunk) {
	bytes, _ := proto.Marshal(chunk)
	sn.storageIO.Persist(sn.storageDir+chunk.ChunkName, bytes)
	sn.statsBoard.AddUploaded()
	if chunk.StorageNodes == nil || len(chunk.StorageNodes) == 0 { // Replicate!
		sn.replicate(chunk)
	}
}

func (sn *StorageNodeImpl) replicate(chunk *m.Chunk) {
	chunk.StorageNodes = make(map[string]*m.Node, 0)
	chunk.StorageNodes[sn.uuid] = &m.Node{
		Uuid:     sn.uuid,
		Hostname: sn.server.GetHostname(),
		Port:     int32(sn.server.GetPort()),
		Stats:    sn.statsBoard.GetAll(),
	}
	if len(sn.replicas) <= CHUNK_REPLICAS {
		for _, replica := range sn.replicas {
			sn.replicateAndUpdateStats(replica, chunk)
		}
	} else {
		// getting 3 random replicas
		// code turned out to be more confusing than should be
		replicas := make([]string, len(sn.replicas))
		i := 0
		for k := range sn.replicas {
			replicas[i] = k
			i++
		}
		isSelected := make(map[int]bool)
		for len(isSelected) < CHUNK_REPLICAS {
			rand.Seed(time.Now().UnixNano())
			randIdx := rand.Intn(len(sn.replicas))
			if _, present := isSelected[randIdx]; !present {
				isSelected[randIdx] = true
				selected := sn.replicas[replicas[randIdx]]
				sn.replicateAndUpdateStats(selected, chunk)
			}
		}
	}
}

func (sn *StorageNodeImpl) replicateAndUpdateStats(node *m.Node, chunk *m.Chunk) {
	msgHandler, err := m.GetMessageHandlerFor(helpers.GetAddr(node.Hostname, int(node.Port)))
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	msgHandler.SendChunkUploadRequest(chunk)
	sn.statsBoard.AddReplicated()
}

func sendStatus(computeEngineConn *m.MessageHandler, computeType m.ComputeType) func(ok bool, err string) {
	var jobStatus m.JobStatus
	if computeType == m.ComputeType_MAP {
		jobStatus = m.JobStatus_job_mappers
	} else if computeType == m.ComputeType_REDUCE {
		jobStatus = m.JobStatus_job_reducers
	}
	return func(ok bool, err string) {
		computeEngineConn.SendComputationStatus(
			jobStatus,
			ok,
			err)
	}
}

/** How do you handle nils in Gol without bloating the code like this function? */
func (sn *StorageNodeImpl) handleMapRequest(
	computeEngineConn *m.MessageHandler,
	actionRequest *m.ActionRequest,
) {
	defer func(computeEngineConn *m.MessageHandler) {
		err := computeEngineConn.Close()
		if err != nil {

		}
	}(computeEngineConn)
	plugin := actionRequest.Plugin
	computeType := actionRequest.ComputeType
	chunkName := actionRequest.FileName
	updateComputeStatus := sendStatus(computeEngineConn, computeType)
	chunkPath := sn.storageDir + chunkName

	/**
	* we need to extract data from local chunk protos and store it somewhere
	* for mapper to read - I'm sorry for this
	 */

	/** Get local chunk */
	file, err := os.ReadFile(chunkPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ChunkName": chunkName}).Error("Error reading local chunk")
		updateComputeStatus(false, "Error reading local chunk")
		return
	}

	/** Parse local byte chunk into actually Chunk message */
	chunk := &m.Chunk{}
	err = proto.Unmarshal(file, chunk)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ChunkName": chunkPath, "ErrorMsg": err.Error()}).Error("Error unmarshalling chunk")
		updateComputeStatus(false, "Error reading local chunk")
		return
	}

	/** Persist data (this is absurd) to compute storage dir */
	dataPath := sn.computeStorageDir + chunkName
	os.MkdirAll(filepath.Dir(dataPath), os.ModePerm)
	err = os.WriteFile(dataPath, chunk.Data, os.ModePerm)
	if err != nil {
		logrus.WithFields(logrus.Fields{"DataPath": dataPath, "ErrorMsg": err.Error()}).Error("Copying chunk data to Compute Storage dir")
		updateComputeStatus(false, "Error handling local chunks")
		return
	}

	/** Persist plugin only if it doesn't exist */
	pluginName := helpers.GetFilename(chunkName) + "-" + helpers.GetFilename(plugin.Name)
	pluginDir := sn.pluginsDir + "/" + pluginName + "-" + helpers.GetFilename(chunkName)
	if _, err := os.Stat(pluginDir); errors.Is(err, os.ErrNotExist) {
		logrus.WithFields(logrus.Fields{"PluginName": pluginName}).Info("Persisting plugin")
		sn.storageIO.Persist(pluginDir, plugin.Plugin)
	}

	/** Preparing Compute Engine */
	context := compute_engine.NewContext(sn.computeStorageDir)
	context.SetNodeUuid(sn.uuid)
	context.SetComputeOutputFilename(chunkName)
	context.SetReducerNodes(actionRequest.Reducers)
	computeEngine := compute_engine.NewComputeEngine(context)

	logrus.WithFields(logrus.Fields{"Phase": computeType.String()}).Info("Starting Run Mapper")
	/** Compute Engine Ready for Execution */
	err = computeEngine.RunMapper(pluginDir, dataPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ChunkName": chunkName, "ErrorMsg": err.Error()}).Error("Mapper phase error")
		updateComputeStatus(false, err.Error())
		return
	}
	logrus.WithFields(logrus.Fields{"Phase": computeType.String()}).Info("Starting Shuffling")
	filesTable := computeEngine.Shuffle()
	logrus.WithFields(logrus.Fields{"ReducerNodes": actionRequest.Reducers}).Info("Shuffling complete")

	/** Notify resource manager of the mapper output files */
	computeEngineConn.SendMappersOutputTable(filesTable)
	logrus.Info("Sent mappers output files table to Resource Manager")

	/** We are done! */
	os.Remove(dataPath)  // perhaps we could have deferred it right under declaration
	os.Remove(pluginDir) // perhaps we could have deferred it right under declaration
	logrus.WithFields(logrus.Fields{"Chunk": chunkName, "Job": computeType.String()}).Info("Compute Complete")
}

func (sn *StorageNodeImpl) storeMapperOutput(
	filename string,
	data []byte,
) {
	logrus.WithFields(logrus.Fields{"Filename": filename}).Info("Persisting Mapper output file")
	helpers.CreatePaths(sn.computeStorageDir)
	filename = sn.computeStorageDir + "/" + filename
	os.WriteFile(filename, data, os.ModePerm)
}

/*
Assume the Reducer has received all of the outputs from the Mapper in sorted order.
First, merge these sorted outputs into a single file.
Then, sort this single merged file of <key, value> pairs into <key, [value]> pairs/
Then, pass this file into the ComputeEngine RunReducer() method.
This method is in charge of parsing key/values and passing them to the plugin.

TODO: refactor this massive function into smaller functions
*/
func (sn *StorageNodeImpl) handleReduceRequest(
	computeEngineConn *m.MessageHandler,
	filenames []string,
	plugin *m.Plugin,
	reducerNumber int32,
	outputFilename string,
) {
	/** converted to absolute paths */
	for i, filename := range filenames {
		filenames[i] = sn.computeStorageDir + "/" + filename
	}
	logrus.Info("Initiating Reduce Phase")
	/* TODO: check if filenames is empty */
	defer computeEngineConn.Close()
	updateComputeStatus := sendStatus(computeEngineConn, m.ComputeType_REDUCE)

	logrus.Info("Initiating merge of Mappers Output")
	/* Maps a mapper output file scanner to the next key in the file */
	mergeMap := make(map[*bufio.Scanner]string)

	/* Create a scanner for each mapper output file and read the first line */
	logrus.WithFields(logrus.Fields{"Filenames": filenames}).Info("Opening mapper outputs")
	files := []*os.File{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		files = append(files, file)
		if err != nil {
			logrus.WithFields(logrus.Fields{"Filename": filename}).Error("Cannot open file")
			updateComputeStatus(false, err.Error())
			return
		}

		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		scanner.Scan()
		mergeMap[scanner] = scanner.Text()
	}

	mergedFilePath := sn.computeStorageDir + plugin.Name + "-mergefile-" + strconv.Itoa(int(reducerNumber))
	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"Filename": mergedFile}).Error("Cannot create merged file")
		updateComputeStatus(false, err.Error())
		return
	}

	/* Merge the Mapper output files into a single file */
	for len(mergeMap) != 0 {
		var smallestScanner *bufio.Scanner
		var smallestText string
		for scanner, text := range mergeMap {
			if smallestText == "" || strings.Compare(text, smallestText) == -1 {
				smallestText = text
				smallestScanner = scanner
			}
		}
		if smallestText != "" {
			mergedFile.WriteString(smallestText + "\n")
		}
		if smallestScanner.Scan() {
			/* There are more lines in this file, update mergeMap */
			mergeMap[smallestScanner] = smallestScanner.Text()
		} else {
			/* There are no more lines in this file, remove it from mergeMap */
			delete(mergeMap, smallestScanner)
		}
	}
	for _, file := range files {
		file.Close()
	}
	mergedFile.Close()

	// TODO: move sorting to separate method
	sortedFilePath := sn.computeStorageDir + plugin.Name + "-sortfile-" + strconv.Itoa(int(reducerNumber))
	sortedFile, err := os.Create(sortedFilePath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"Filename": sortedFile}).Error("Cannot create sorted file")
		updateComputeStatus(false, err.Error())
		return
	}
	mergedFile, _ = os.Open(mergedFilePath)
	scanner := bufio.NewScanner(mergedFile)
	var currentOutputKey string
	var outputLine string
	for scanner.Scan() {
		inputLine := scanner.Text()
		i := 0
		for j, v := range inputLine { // get index of last tab
			if string(v) == "\t" {
				i = j
			}
		}
		inputLineKey := inputLine[:i]
		inputLineValue := inputLine[i+1:]

		// TODO: create a slice to store values list instead of string concatenation
		if strings.Compare(inputLineKey, currentOutputKey) == 0 {
			outputLine += "\t" + inputLineValue
		} else {
			currentOutputKey = inputLineKey
			if outputLine != "" {
				sortedFile.WriteString(outputLine + "\n")
			}
			outputLine = inputLineKey + "\t" + inputLineValue
		}
	}
	for _, filename := range filenames {
		logrus.WithFields(logrus.Fields{"Filename": filename}).Info("Deleting output mapper file")
		os.Remove(filename)
	}
	mergedFile.Close()
	sortedFile.Close()
	logrus.Info("Merged mappers output files")

	/** Persist plugin only if it doesn't exist */
	pluginDir := sn.pluginsDir + plugin.Name + "-reducer-" + strconv.Itoa(int(reducerNumber))
	if _, err := os.Stat(pluginDir); errors.Is(err, os.ErrNotExist) {
		logrus.WithFields(logrus.Fields{"Path": pluginDir}).Info("New plugin")
		sn.storageIO.Persist(pluginDir, plugin.Plugin)
	}

	/** Preparing Compute Engine */
	context := compute_engine.NewContext(sn.computeStorageDir)
	outputFilePath := outputFilename + "-" + strconv.Itoa(int(reducerNumber))
	context.SetComputeOutputFilename(outputFilePath)
	computeEngine := compute_engine.NewComputeEngine(context)

	/** Compute Engine Ready for Execution */
	logrus.WithFields(logrus.Fields{"Phase": m.ComputeType_REDUCE.String()}).Info("Running reduce compute")
	reduceErr := computeEngine.RunReducer(pluginDir, sortedFilePath)

	if reduceErr == nil {
		logrus.Info("Reducer completed successfully")
		updateComputeStatus(true, "")
	} else {
		logrus.WithFields(logrus.Fields{"Error": reduceErr.Error()}).Error("Error in reducer")
		updateComputeStatus(false, reduceErr.Error())
	}

	logrus.WithFields(logrus.Fields{"File": outputFilePath}).Info("Compute complete!")

	/** Cleanup intermediate files */
	os.Remove(mergedFilePath)
	os.Remove(sortedFilePath)
	os.Remove(pluginDir)

	/* Upload reducer output to DFS */
	msgHandler, err := m.GetMessageHandlerFor(sn.controllerAddr)
	if err != nil {
		logrus.Error("Controller down. Going to standby mode.")
		sn.stopHeartbeats()
		return
	}
	msgHandler.SendPUTRequest(outputFilePath)
	wrapper, _ := msgHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_AckMessage:
		errorMsg := msg.AckMessage.ErrorMessage
		logrus.Error(errorMsg)
	case *m.Wrapper_StorageNodesMessage:
		storageNodes := msg.StorageNodesMessage.Nodes
		chunkinator := c.NewChunkinator(context.GetComputeOutputFilename(), outputFilePath)
		uploader := c.NewUploader(storageNodes, chunkinator)
		err := uploader.Upload()
		if err != nil {
			logrus.Error("Upload error! " + err.Error())
		} else {
			logrus.Info("Reducer output uploaded to DFS successfully")
		}
	case nil:
		if err := msgHandler.Close(); err != nil {
			logrus.Error(err.Error())
		}
	default:
		logrus.Error("Unrecognized response from server")
	}
}

func (sn *StorageNodeImpl) worker() {
	for {
		select {
		case <-sn.heartbeatStatusCh: // stops heartbeats
			return
		case <-sn.heartbeatScheduler.C:
			sn.handleHeartbeat()
			logrus.WithFields(logrus.Fields{"controllerAddr": sn.controllerAddr}).Info("Sent heartbeat")
		case replicas := <-sn.replicasCh:
			sn.handleReplicaTable(replicas)
		}
	}
}

/** Initiates heartbeats scheduler. On tick will send heartbeat to controller */
func (sn *StorageNodeImpl) Heartbeats() {
	sn.heartbeatScheduler = time.NewTicker(HEARTBEAT_DELAY_S * time.Second)
	sn.heartbeatStatusCh = make(chan bool)
}
func (sn *StorageNodeImpl) handleHeartbeat() {
	msgHandler, err := m.GetMessageHandlerFor(sn.controllerAddr)
	if err != nil {
		logrus.Error("Controller down. Going to standby mode.")
		sn.stopHeartbeats()
		return
	}
	e := msgHandler.SendHeartbeat(
		sn.uuid,
		sn.server.GetHostname(),
		sn.server.GetPort(),
		sn.storageIO.ScanMetadata(sn.storageDir),
		sn.statsBoard.GetAll(),
	)
	if e != nil {
		sn.stopHeartbeats()
		logrus.Error("Controller down. Going to standby mode.")
		return
	}

	// expects in response information about other storageIO nodes in cluster
	// * ideally, implement timeout here
	wrapper, err := msgHandler.Receive()
	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_StorageNodesMessage:
		go func() {
			sn.replicasCh <- msg.StorageNodesMessage.Nodes
		}()
	default:
		logrus.Error("Expected Online StorageIO Nodes back from Heartbeat")
	}
	msgHandler.Close()
}

func (sn *StorageNodeImpl) handleReplicaTable(replicas []*m.Node) {
	// replica table gets overwritten every heartbeat
	sn.replicas = make(map[string]*m.Node)
	for _, replica := range replicas {
		if replica.Uuid == sn.uuid {
			continue // don't add itself as replica
		}
		sn.replicas[replica.Uuid] = replica
	}
}

func (sn *StorageNodeImpl) stopHeartbeats() {
	sn.heartbeatScheduler.Stop()
	sn.heartbeatStatusCh <- false // stops sending heartbeats
}

func (sn *StorageNodeImpl) ClusterStats() *m.Stats {
	return sn.statsBoard.GetAll()
}
