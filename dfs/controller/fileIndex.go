package controller

import (
	"adfs/helpers"
	m "adfs/messages"
	"errors"
	"strconv"

	"github.com/sirupsen/logrus"
)

type FileIndex interface {
	Start()
	Stop()
	Ls() []*FileMetadata
	Get(filename string) (*m.File, error)
	Put(fileIndex *m.Chunk)
	PutAll(storageNode *m.Node, chunks []*m.Chunk)
	Rm(filename string) error
	ReserveSlot(filename string)
	FileExists(filename string) bool
	NodeDown(nodeUuid string)
}

type FileIndexImpl struct {
	index            map[string]*FileMetadata // [dirname] filemetadata  /folder1/test.img
	pendingUploads   map[string]bool
	updateIndexChan  chan *StorageNodeUpdate
	rmFileCh         chan string
	pendingUploadsCh chan string
	nodeDownCh       chan string
}

type FileMetadata struct {
	filename string
	chunks   map[string]*m.Chunk // [chunkName] chunkInformation
}

type StorageNodeUpdate struct {
	storageNode *m.Node
	chunks      []*m.Chunk
}

func NewFileIndex() FileIndex {
	return &FileIndexImpl{
		index:            make(map[string]*FileMetadata),
		pendingUploads:   make(map[string]bool),
		updateIndexChan:  make(chan *StorageNodeUpdate),
		rmFileCh:         make(chan string),
		pendingUploadsCh: make(chan string),
		nodeDownCh:       make(chan string),
	}
}

func (f *FileIndexImpl) Start() {
	go f.worker()
}

// TODO: stop gracefully
func (f *FileIndexImpl) Stop() {}

func (f *FileIndexImpl) worker() {
	for {
		select {
		case storageNodeUpdate := <-f.updateIndexChan:
			f.handleStorageNodeUpdate(storageNodeUpdate)
		case filename := <-f.rmFileCh:
			delete(f.index, filename)
			fields := logrus.Fields{}
			i := 0
			for _, f := range f.index {
				fields[strconv.Itoa(i)] = f.filename
				i++
			}
			logrus.WithFields(fields).Info("Current files: ")
		case filename := <-f.pendingUploadsCh:
			f.pendingUploads[filename] = true
		case nodeUuid := <-f.nodeDownCh:
			f.handleNodeDown(nodeUuid)
		}
	}
}

// this function is nasty
func (f *FileIndexImpl) handleStorageNodeUpdate(storageNodeUpdate *StorageNodeUpdate) {
	for _, newChunk := range storageNodeUpdate.chunks {
		sn := storageNodeUpdate.storageNode
		newStorageNodes := map[string]*m.Node{
			sn.Uuid: sn}

		newFilename := newChunk.FileName
		if _, prst := f.pendingUploads[newFilename]; prst {
			delete(f.pendingUploads, newFilename)
		}

		file, present := f.index[newFilename]
		newChunk.StorageNodes = newStorageNodes
		// case: file doesn't exist on file index
		if !present {
			chunks := map[string]*m.Chunk{newChunk.ChunkName: newChunk}
			fileMetadata := &FileMetadata{
				filename: newChunk.FileName,
				chunks:   chunks,
			}
			f.index[newFilename] = fileMetadata
		} else { // case: file exists in index
			chunks := file.chunks
			// case: chunk exists in file metadata...
			if chunk, present := chunks[newChunk.ChunkName]; present {
				// case: chunk exists in file of file index
				if _, present := chunk.StorageNodes[sn.Uuid]; present {
					// case: storage node is registered as owner of chunk
					continue
				} else {
					// case: storage node is not registered as owner of chunk
					chunk.StorageNodes[sn.Uuid] = sn
				}
			} else {
				// case: chunk doesn't exist in file of file index
				chunks[newChunk.ChunkName] = newChunk
			}
		}
	}
}

func (f *FileIndexImpl) Ls() []*FileMetadata {
	metadata := []*FileMetadata{}
	for _, v := range f.index {
		metadata = append(metadata, v)
	}
	return metadata
}

func (f *FileIndexImpl) Get(filename string) (*m.File, error) {
	_, exists := f.index[filename]
	if !exists {
		return nil, errors.New(filename + " doesn't exist")
	}
	chunks := []*m.Chunk{}
	for _, c := range f.index[filename].chunks {
		chunks = append(chunks, c)
	}
	return &m.File{
		Name:    helpers.GetFilename(filename),
		Dirname: filename,
		Chunks:  chunks,
	}, nil
}

func (f *FileIndexImpl) Put(chunk *m.Chunk) {
	// Currently not required by implementation
	// Created method for future needs
}

func (f *FileIndexImpl) PutAll(node *m.Node, chunks []*m.Chunk) {
	storageNodeUpdate := &StorageNodeUpdate{
		storageNode: node,
		chunks:      chunks,
	}
	f.updateIndexChan <- storageNodeUpdate
}

func (f *FileIndexImpl) Rm(filename string) error {
	f.rmFileCh <- filename
	return nil
}

func (f *FileIndexImpl) FileExists(filename string) bool {
	_, presentIndex := f.index[filename]
	_, presentPending := f.pendingUploads[filename]
	return presentIndex || presentPending
}

func (f *FileIndexImpl) ReserveSlot(filename string) {
	f.pendingUploadsCh <- filename
}

func (f *FileIndexImpl) NodeDown(nodeUuid string) {
	f.nodeDownCh <- nodeUuid
}

func (f *FileIndexImpl) handleNodeDown(nodeUuid string) {
	for _, file := range f.index {
		for _, chunk := range file.chunks {
			if _, present := chunk.StorageNodes[nodeUuid]; present {
				delete(chunk.StorageNodes, nodeUuid)
			}
		}
	}
}

func (f *FileIndexImpl) PrintIndex() {
	p := "\n"
	for filename, file := range f.index {
		p += filename + "\n"
		for _, chunk := range file.chunks {
			p += "	* " + chunk.ChunkName + "\n"
			for _, sn := range chunk.StorageNodes {
				p += "		- " + helpers.GetAddr(sn.Hostname, int(sn.Port)) + "\n"
			}
		}
	}
	logrus.Info(p)
}
