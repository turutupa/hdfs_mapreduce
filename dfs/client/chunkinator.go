package client

import (
	"adfs/common"
	m "adfs/messages"
	"bufio"
	"io"
	"os"
	"strconv"
)

type Chunkinator interface {
	Chunk() (*m.Chunk, error)
}

type ChunkinatorImpl struct {
	localFilename       string
	destinationFilename string
	numChunks           int
	serial              int32
	offset              int
	fileSize            int32
}

func NewChunkinator(localFilename, destinationFilename string) Chunkinator {
	c := &ChunkinatorImpl{
		localFilename:       localFilename,
		destinationFilename: destinationFilename,
		serial:              0,
		offset:              0,
	}
	return c
}

func (c *ChunkinatorImpl) Chunk() (*m.Chunk, error) {
	data, err := read(c.localFilename, int64(c.offset))
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	chunk := &m.Chunk{
		FileName:  c.destinationFilename,
		ChunkName: c.destinationFilename + "-" + strconv.Itoa(int(c.serial)),
		Serial:    c.serial,
		Size:      int64(len(data)),
		Data:      data,
		Offset:    int32(c.offset),
		FileSize:  int32(c.fileSize),
	}
	c.serial++
	c.offset = c.offset + len(data)

	return chunk, nil
}

func read(filename string, offset int64) ([]byte, error) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	if fileInfo, e := file.Stat(); e != nil || fileInfo.Size() < offset {
		// error or done
		return nil, nil
	}
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	data := make([]byte, common.CHUNK_SIZE)
	// TODO: create only one reader
	reader := bufio.NewReader(file)
	numBytes, err := reader.Read(data)

	// something went wrong
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	// chunk is smaller than expected chunk size means
	// we get rid of empty bytes. Improvable.
	if int64(numBytes) < common.CHUNK_SIZE {
		d := make([]byte, numBytes)
		for i := 0; i < numBytes; i++ {
			d[i] = data[i]
		}
		return d, nil
	}

	// If the last character is not a new line,
	// then continue to read until a new line
	// character is found
	if data[len(data)-1] != '\n' {
		dataSlice, err := reader.ReadBytes('\n')
		data = append(data, dataSlice...)

		// something went wrong
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
	}

	return data, nil
}
