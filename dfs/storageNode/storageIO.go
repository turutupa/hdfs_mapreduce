package storageNode

import (
	"adfs/helpers"
	m "adfs/messages"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type StorageIO interface {
	Persist(filename string, data []byte)
	ScanMetadata(dir string) []*m.Chunk
	Retrieve(filename string) ([]byte, error)
	Delete(filename string) error
}

type StorageIOImpl struct{}

func NewStorageIO() StorageIO {
	return &StorageIOImpl{}
}

/** Creates a file and all the folders in the path */
func (sio *StorageIOImpl) Persist(filename string, data []byte) {
	err := helpers.CreatePaths(helpers.GetPathFrom(filename))
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	err = os.WriteFile(filename, data, os.ModePerm)
	if err != nil {
		logrus.Error(err.Error())
	}
}

/** This is not adding any value */
func (sio *StorageIOImpl) Retrieve(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

/** This is not adding any value */
func (sio *StorageIOImpl) Delete(filename string) error {
	return os.Remove(filename)
}

func (sio *StorageIOImpl) ScanMetadata(storageDir string) []*m.Chunk {
	var localFiles []*m.Chunk
	localFiles, err := sio.scanDir(localFiles, storageDir)
	if err != nil { // added this in case scanning happens while file is being deleted
		<-time.After(1 * time.Second)
		return sio.ScanMetadata(storageDir)
	}
	return localFiles
}

func (sio *StorageIOImpl) scanDir(localFiles []*m.Chunk, dirname string) ([]*m.Chunk, error) {
	entries := helpers.GetDirEntries(dirname)
	for _, f := range entries {
		subDir := dirname + "/" + f.Name()
		if f.IsDir() {
			subDirFiles, err := sio.scanDir(localFiles, subDir)
			if err != nil {
				return nil, err
			}
			localFiles = append(localFiles, subDirFiles...)
		} else {
			chunk, err := readProtoMetadata(subDir)
			if err != nil {
				return nil, err
			}
			localFiles = append(localFiles, chunk)
		}
	}
	return localFiles, nil
}

func readProtoMetadata(filename string) (*m.Chunk, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	chunk := &m.Chunk{}
	err = proto.Unmarshal(data, chunk)
	if err != nil {
		return nil, err
	} else {
		// we only want Filename + chunk-name
		// we want this information to send to controller.
		// controller will add this node as owner of chunk.
		// So we're sending all the info BUT the actual data
		return &m.Chunk{
			FileName:  chunk.FileName,
			ChunkName: chunk.ChunkName,
			Serial:    chunk.Serial,
			Size:      chunk.Size,
			Offset:    chunk.Offset,
			FileSize:  chunk.FileSize,
		}, nil
	}
}
