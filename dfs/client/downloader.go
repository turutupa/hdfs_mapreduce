package client

import (
	"adfs/helpers"
	m "adfs/messages"
	"bufio"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
)

const TEMP_DIR = "/.temp"

type Downloader interface {
	Download() error
}

type DownloaderImpl struct {
	storageDir          string     // downloads dir
	tempDir             string     // temp folder to store chunks
	filename            string     // filename of file to be downloaded
	dirname             string     // complete dirname of file to be downloaded
	chunks              []*m.Chunk // chunks to download
	chunksDownloaded    int
	tempChunksPersisted chan bool // to wait for all chunks to be persisted before merging
	chunksCh            chan *m.Chunk
	done                chan bool
}

func NewDownloader(storageDir string, filename string, chunks []*m.Chunk) Downloader {
	return &DownloaderImpl{
		storageDir:          storageDir,
		tempDir:             storageDir + TEMP_DIR,
		filename:            filename,
		chunks:              chunks,
		chunksDownloaded:    0,
		tempChunksPersisted: make(chan bool),
		chunksCh:            make(chan *m.Chunk),
		done:                make(chan bool),
	}
}

func (d *DownloaderImpl) Download() error {
	// create download and temp storage folders
	err := helpers.CreatePaths(d.storageDir)
	if err != nil {
		return err
	}
	err = helpers.CreatePaths(d.tempDir)
	if err != nil {
		return err
	}
	if string(d.filename[0]) == "/" {
		d.dirname = d.storageDir + d.filename
	} else {
		d.dirname = d.storageDir + "/" + d.filename
	}
	err = helpers.CreatePaths(helpers.GetPathFrom(d.dirname))

	// init worker
	go d.worker()
	defer d.close()

	logrus.Info("Downloading init! Sit tight!")
	// init download
	err = d.downloadChunks()
	if err != nil {
		return err
	}

	d.waitDownloads()

	logrus.Info("Downloaded successfully... creating file")
	err = d.mergeChunks()
	if err != nil {
		return err
	}
	return nil
}

func (d *DownloaderImpl) close() {
	d.done <- true
}

// None optimized solution for downloading chunks
// A connection is opened per chunk at the same time,
// doesn't even check if it is opening connections to the
// same node. If this project makes it out there I will optimize
func (d *DownloaderImpl) downloadChunks() error {
	var err error
	var wg sync.WaitGroup
	wg.Add(len(d.chunks))
	for _, c := range d.chunks {
		chunk := c
		storageNodes := chunk.StorageNodes
		go func() {
			defer wg.Done()
			if len(storageNodes) == 0 {
				err = errors.New("chunk doesn't have available Storage Nodes")
				return
			}
			// sorts storage nodes in terms of how much data they've transferred
			// with the attempt to have them all equally busy
			//sort.Slice(storageNodes, func(i, j int) bool {
			//	return storageNodes[i].Stats.Downloaded > storageNodes[j].Stats.Downloaded
			//})
			for _, sn := range storageNodes {
				addr := helpers.GetAddr(sn.Hostname, int(sn.Port))
				msgHandler, err := m.GetMessageHandlerFor(addr)
				if err != nil {
					return
				}
				err = msgHandler.SendChunkDownloadRequest(chunk.ChunkName)
				wrapper, _ := msgHandler.Receive()
				switch msg := wrapper.Msg.(type) {
				case *m.Wrapper_AckMessage:
					if !msg.AckMessage.Ok {
						fmt.Println(fail("Error from " + sn.Hostname + ":" + strconv.Itoa(int(sn.Port)) + ": " + msg.AckMessage.ErrorMessage))
					}
				case *m.Wrapper_ChunkMessage:
					d.chunksCh <- msg.ChunkMessage
					return
				}
				if err = msgHandler.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}
			// chunk was not downloaded
			err = errors.New("Error trying to retrieve chunk " + chunk.ChunkName + "\n" + err.Error())
		}()
	}
	wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (d *DownloaderImpl) mergeChunks() error {
	sort.Slice(d.chunks, func(i, j int) bool {
		return d.chunks[i].Serial < d.chunks[j].Serial
	})
	_, err := os.Create(d.dirname)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(d.dirname, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	var writePosition int64 = 0
	for _, chunk := range d.chunks {
		tempChunkName := d.tempDir + "/" + helpers.GetFilename(chunk.ChunkName)
		newFileChunk, err := os.Open(tempChunkName)
		if err != nil {
			os.Remove(d.dirname)
			return err
		}
		defer newFileChunk.Close()
		defer os.Remove(tempChunkName)
		chunkInfo, err := newFileChunk.Stat()
		if err != nil {
			return err
		}
		chunkSize := chunkInfo.Size()
		chunkBufferBytes := make([]byte, chunkSize)

		writePosition = writePosition + chunkSize
		reader := bufio.NewReader(newFileChunk)
		_, err = reader.Read(chunkBufferBytes)
		if err != nil {
			return err
		}
		_, err = file.Write(chunkBufferBytes)
		if err != nil {
			return err
		}
		file.Sync()
		chunkBufferBytes = nil
	}
	file.Close()
	return nil
}

func (d *DownloaderImpl) worker() {
	for {
		select {
		case <-d.done:
			return
		case chunk := <-d.chunksCh:
			err := d.writeChunk(chunk)
			if err != nil {
				logrus.Error(err.Error())
			}
			d.chunksDownloaded++
			go func() {
				d.tempChunksPersisted <- true
			}()
		}
	}
}

func (d *DownloaderImpl) writeChunk(chunk *m.Chunk) error {
	dirname := d.tempDir + "/" + helpers.GetFilename(chunk.ChunkName)
	err := os.WriteFile(dirname, chunk.Data, os.ModePerm)
	if err != nil {
		logrus.Error("ERROR: " + err.Error())
		return err
	}
	return nil
}

// every time a chunk is persisted it sends a "true" signal -
// after all chunks can be confirmed (persisted) merging starts
func (d *DownloaderImpl) waitDownloads() {
	for i := 0; i < len(d.chunks); i++ {
		<-d.tempChunksPersisted
	}
}
