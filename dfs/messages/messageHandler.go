package messages

import (
	"encoding/binary"
	"net"

	"google.golang.org/protobuf/proto"
)

type MessageHandler struct {
	conn     net.Conn
	IsClosed bool
}

const TCP = "tcp"

func (m *MessageHandler) GetRemoteAddr() net.Addr {
	return m.conn.RemoteAddr()
}

func GetMessageHandlerFor(addr string) (*MessageHandler, error) {
	conn, err := net.Dial(TCP, addr)
	if err != nil {
		return nil, err
	}

	return NewMessageHandler(conn), nil
}

func (m *MessageHandler) Close() error {
	if err := m.conn.Close(); err != nil {
		return err
	}
	m.IsClosed = true
	return nil
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	return &MessageHandler{
		conn: conn,
	}
}

func (m *MessageHandler) readN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) writeN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	if err := m.writeN(prefix); err != nil {
		return err
	}
	if err := m.writeN(serialized); err != nil {
		return err
	}
	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload)

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) Redirect(wrapper *Wrapper) error {
	return m.Send(wrapper)
}

func (m *MessageHandler) SendRegistrationMessage(uuid, hostname string, port int) error {
	node := &Node{
		Uuid:     uuid,
		Hostname: hostname,
		Port:     int32(port),
	}
	registrationMessage := &Registration{Node: node}
	wrapper := &Wrapper{
		Msg: &Wrapper_RegistrationMessage{
			RegistrationMessage: registrationMessage,
		}}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendHeartbeat(
	uuid string,
	hostname string,
	port int,
	chunks []*Chunk,
	stats *Stats,
) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_HeartbeatMessage{
			HeartbeatMessage: &Heartbeat{
				Chunks: chunks,
				StorageNode: &Node{
					Uuid:     uuid,
					Hostname: hostname,
					Port:     int32(port),
				},
				Stats: stats,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendLSRequest() error {
	return m.sendActionRequest(ActionType_LS, "", "", nil)
}

func (m *MessageHandler) SendPUTRequest(filename string) error {
	return m.sendActionRequest(ActionType_PUT, filename, "", nil)
}

func (m *MessageHandler) SendGETRequest(filename string) error {
	return m.sendActionRequest(ActionType_GET, filename, "", nil)
}

func (m *MessageHandler) SendRMRequest(filename string) error {
	return m.sendActionRequest(ActionType_RM, filename, "", nil)
}

func (m *MessageHandler) SendChunkRemoveRequest(chunkName string) error {
	return m.sendActionRequest(ActionType_RM, "", chunkName, nil)
}

func (m *MessageHandler) SendChunkDownloadRequest(chunkName string) error {
	return m.sendActionRequest(ActionType_GET, "", chunkName, nil)
}

func (m *MessageHandler) SendChunkUploadRequest(chunk *Chunk) error {
	return m.sendActionRequest(ActionType_PUT, "", "", chunk)
}

func (m *MessageHandler) SendClusterStatsRequest() error {
	return m.sendActionRequest(ActionType_CLUSTER_STATS, "", "", nil)
}

func (m *MessageHandler) SendComputeRequest(
	targetFilename string,
	pluginName string,
	outputFilename string,
	plugin []byte,
	computeType ComputeType,
	reducers []*Node,
) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ActionRequestMessage{
			ActionRequestMessage: &ActionRequest{
				Type:     ActionType_COMPUTE,
				FileName: targetFilename,
				Plugin: &Plugin{
					Name:   pluginName,
					Plugin: plugin,
				},
				ComputeType: computeType,
				Reducers:    reducers,
				OutputFilename: outputFilename,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendReduceRequest(
	targetFilenames []string,
	pluginName string,
	plugin []byte,
	reducerNumber int32,
	outputFilename string,
) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ActionRequestMessage{
			ActionRequestMessage: &ActionRequest{
				Type: ActionType_COMPUTE,
				Plugin: &Plugin{
					Name:   pluginName,
					Plugin: plugin,
				},
				ComputeType: ComputeType_REDUCE,
				FileNames:   targetFilenames,
				ReducerNumber: reducerNumber,
				OutputFilename: outputFilename,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendComputeStore(
	filename string,
	file []byte,
) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ActionRequestMessage{
			ActionRequestMessage: &ActionRequest{
				Type:     ActionType_COMPUTE_STORE,
				FileName: filename,
				Data:     file,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) sendActionRequest(requestType ActionType, filename, chunkName string, chunk *Chunk) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ActionRequestMessage{
			ActionRequestMessage: &ActionRequest{
				Type:      requestType,
				FileName:  filename,
				ChunkName: chunkName,
				Chunk:     chunk,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendChunk(chunk *Chunk) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ChunkMessage{
			ChunkMessage: chunk,
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendNodes(nodes []*Node) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_StorageNodesMessage{
			StorageNodesMessage: &StorageNodes{
				Nodes: nodes,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendFilesMetadata(files []*File) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_FilesMessage{
			FilesMessage: &Files{
				Files: files,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendFileMetadata(file *File) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_FileMessage{
			FileMessage: file,
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendComputationStatus(
	status JobStatus,
	ok bool,
	errorMessage string,
) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ComputationStatusMessage{
			ComputationStatusMessage: &ComputationStatus{
				Status:       status,
				Ok:           ok,
				ErrorMessage: errorMessage,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendMappersOutputTable(filesTable map[string]*Node) error {
	wrapper := &Wrapper{
		Msg: &Wrapper_ComputationStatusMessage{
			ComputationStatusMessage: &ComputationStatus{
				Status:       JobStatus_job_mappers,
				Ok:           true,
				ErrorMessage: "",
				FilesTable:   filesTable,
			},
		},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendSuccessAck() {
	m.sendAck(true, "")
}

func (m *MessageHandler) SendFailAck(errorMsg string) {
	m.sendAck(false, errorMsg)
}

func (m *MessageHandler) sendAck(ok bool, msg string) {
	m.Send(&Wrapper{
		Msg: &Wrapper_AckMessage{
			AckMessage: &Ack{Ok: ok, ErrorMessage: msg},
		},
	})
}
