package client

import (
	"adfs/helpers"
	m "adfs/messages"
	"errors"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type Actions interface {
	Upload(localDirname, remoteDirname string)
	Download(localDirname, remoteDirname string)
	Delete(filename string)
	List() ([]*m.File, error)
	GetClusterStats() ([]*m.Node, error)
	Compute(localJobName, remoteFilename, outputFilename string)
}

type ActionsImpl struct {
	controllerAddr string
	storageDir     string
}

func NewActions(controllerAddr, storageDir string) Actions {
	return &ActionsImpl{controllerAddr, storageDir}
}

func (a *ActionsImpl) Upload(localDirname, remoteDirname string) {
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		dialog(fail(CONNECTION_ERROR_MSG))
		return
	}
	msgHandler.SendPUTRequest(remoteDirname)
	wrapper, _ := msgHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_AckMessage:
		errorMsg := msg.AckMessage.ErrorMessage
		dialog("\n\n\t❌" + errorMsg + " ❌")
	case *m.Wrapper_StorageNodesMessage:
		storageNodes := msg.StorageNodesMessage.Nodes
		chunkinator := NewChunkinator(localDirname, remoteDirname)
		uploader := NewUploader(storageNodes, chunkinator)
		err := uploader.Upload()
		if err != nil {
			dialogAppend(fail("Upload error! " + err.Error()))
		} else {
			dialogAppend(success("File uploaded successfully"))
		}
	case nil:
		if err := msgHandler.Close(); err != nil {
			logrus.Error(err.Error())
		}
	default:
		dialog(centered("Unrecognized response from server"))
	}
}

func (a *ActionsImpl) Download(saveAs, remoteDirname string) {
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		dialog(fail(CONNECTION_ERROR_MSG))
		return
	}
	msgHandler.SendGETRequest(remoteDirname)
	wrapper, _ := msgHandler.Receive()
	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_FileMessage:
		chunks := msg.FileMessage.Chunks
		downloader := NewDownloader(a.storageDir, saveAs, chunks)
		err := downloader.Download()
		if err != nil {
			dialogAppend(fail(err.Error()))
			return
		} else {
			dialogAppend(success("File downloaded successfully!"))
			return
		}
	case *m.Wrapper_AckMessage:
		dialog(fail(msg.AckMessage.ErrorMessage))
	case nil:
		dialog(centered("Server said what?"))
		if err := msgHandler.Close(); err != nil {
			logrus.Error(err.Error())
		}
	default:
		errorMsg := "Something went wrong, please try again in a few minutes"
		dialog(centered(errorMsg))
	}
}

func (a *ActionsImpl) Delete(remoteFilename string) {
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		dialog(fail(err.Error()))
		return
	}
	msgHandler.SendRMRequest(remoteFilename)
	wrapper, _ := msgHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_AckMessage:
		if msg.AckMessage.Ok {
			dialogAppend(success("File deleted successfully"))
		} else {
			dialogAppend(fail(msg.AckMessage.ErrorMessage))
		}
	case nil:
		if err := msgHandler.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}
}

func (a *ActionsImpl) List() ([]*m.File, error) {
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		return nil, err
	}
	msgHandler.SendLSRequest()
	wrapper, _ := msgHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_FilesMessage:
		return msg.FilesMessage.Files, nil
	default:
		return nil, errors.New("something went wrong retrieving files")
	}
}

func (a *ActionsImpl) GetClusterStats() ([]*m.Node, error) {
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		return nil, errors.New("A-DFS is not online")
	}
	msgHandler.SendClusterStatsRequest()
	wrapper, _ := msgHandler.Receive()

	switch msg := wrapper.Msg.(type) {
	case *m.Wrapper_StorageNodesMessage:
		return msg.StorageNodesMessage.Nodes, nil
	}

	return nil, errors.New("something went wrong")
}

func (a *ActionsImpl) Compute(localJobName, remoteFilename, outputFilename string) {
	plugin, err := os.ReadFile(localJobName)
	if err != nil {
		dialog(fail(err.Error()))
		return
	}
	msgHandler, err := m.GetMessageHandlerFor(a.controllerAddr)
	if err != nil {
		dialog(fail(err.Error()))
		return
	}
	dialogAppend("")
	logrus.Info("Computation Request Initiated.")
	msgHandler.SendComputeRequest(
		remoteFilename,
		"/"+helpers.GetFilename(localJobName),
		outputFilename,
		plugin,
		m.ComputeType_MAP,
		nil, // these are reducers - determined by compute engine resource manager
	)
	for {
		wrapper, _ := msgHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *m.Wrapper_ComputationStatusMessage:
			if msg.ComputationStatusMessage.Ok {
				if msg.ComputationStatusMessage.Status == m.JobStatus_job_done {
					dialog(success("Computation Job Successful"))
					return
				}
				logrus.WithFields(logrus.Fields{
					"OK":    true,
					"Phase": msg.ComputationStatusMessage.Status,
					"Msg":   msg.ComputationStatusMessage.ErrorMessage,
				}).Info("Computation Status successful")
			} else {
				dialog(fail(msg.ComputationStatusMessage.ErrorMessage))
				return
			}
		default:
			logrus.WithFields(logrus.Fields{
				"ErrorMsg": "No response from server. Connection closed.",
			}).Error("Something went wrong")
			<-time.After(3 * time.Second)
			return
		}
	}
}
