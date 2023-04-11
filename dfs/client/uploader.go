package client

import (
	h "adfs/helpers"
	m "adfs/messages"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
)

type Uploader interface {
	Upload() error
}

type UploaderImpl struct {
	chunkinator  Chunkinator
	storageNodes []*m.Node
	msgHandlers  []*m.MessageHandler
}

func NewUploader(
	storageNodes []*m.Node,
	chunkinator Chunkinator,
) Uploader {
	// sorts storage nodes in terms of how many chunks it has stored
	// so that it tries to even out all nodes
	sort.Slice(storageNodes, func(i, j int) bool {
		return storageNodes[i].Stats.Uploaded < storageNodes[j].Stats.Uploaded
	})
	return &UploaderImpl{
		chunkinator:  chunkinator,
		storageNodes: storageNodes,
		msgHandlers:  make([]*m.MessageHandler, len(storageNodes)),
	}
}

func (u *UploaderImpl) Upload() error {
	u.msgHandlers = h.GetMessageHandlers(u.storageNodes)
	err := u.handleFileUpload()
	if err != nil {
		return err
	}
	h.CloseMessageHandlers(u.msgHandlers)
	return nil
}

// Round-Robin: upload does not follow a fancy algorithm; it is simply
// going to spread chunks in a round-robin fashion "ensuring" a stable
// congestion flow.
// TODO: (optional) add progress bar - ran out of time
func (u *UploaderImpl) handleFileUpload() error {
	chunkCounter := 1
	i := 0
	var wg sync.WaitGroup

	logrus.Info("Uploading! Sit tight!")
	for {
		chunk, err := u.chunkinator.Chunk()
		if err != nil {
			return err // something went wrong
		}
		if chunk == nil || chunk.Size == 0 {
			wg.Wait()
			return nil // we are done!
		}
		msgHandler := u.msgHandlers[i]
		if msgHandler != nil {
			wg.Add(1)
			go u.sendChunk(msgHandler, chunk, &wg)
		}
		chunkCounter++
		i++
		if i >= len(u.msgHandlers) {
			wg.Wait()
			i = 0
		}
	}
}

func (u *UploaderImpl) sendChunk(
	msgHandler *m.MessageHandler,
	chunk *m.Chunk,
	wg *sync.WaitGroup,
) {
	c := &m.Chunk{
		FileName:  chunk.FileName,
		ChunkName: chunk.ChunkName,
		Serial:    chunk.Serial,
		Size:      chunk.Size,
		Data:      chunk.Data,
		Offset:    chunk.Offset,
		FileSize:  chunk.FileSize,
	}
	err := msgHandler.SendChunkUploadRequest(c)
	if err != nil {
		dialog(err.Error())
		// TODO: handle error sending chunk
	} else {
		wg.Done()
	}
}
