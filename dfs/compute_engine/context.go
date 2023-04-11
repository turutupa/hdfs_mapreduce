package compute_engine

import (
	"adfs/helpers"
	m "adfs/messages"
	"errors"
	"hash/fnv"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type Context interface {
	Write(key []byte, value []byte)
	Emit(key []byte, value []byte)
	GetMapperOutputFiles() map[string]int
	SetComputeOutputFilename(filename string)
	GetComputeOutputFilename() string
	SetReducerNodes(reducers []*m.Node)
	GetReducers() []*m.Node
	GetStorageDir() string
	SetNodeUuid(uuid string)
	GetNodeUuid() string
	ClearKeysTracker()
}

const MAP = "MAP"
const REDUCE = "REDUCE"

type ContextImpl struct {
	uuid              string
	storageDir        string
	outputFilePath    string
	reducerNodes      []*m.Node
	mapperOutputFiles map[string]int
	keysTracker       map[string]string
}

func NewContext(storageDir string) Context {
	return &ContextImpl{
		storageDir:        storageDir,
		mapperOutputFiles: make(map[string]int),
		keysTracker:       make(map[string]string),
	}
}

func (c *ContextImpl) SetNodeUuid(uuid string) {
	c.uuid = uuid
}

func (c *ContextImpl) GetNodeUuid() string {
	return c.uuid
}

/** Both mapper and reducer will only have 1 output file */
func (c *ContextImpl) SetComputeOutputFilename(filename string) {
	c.outputFilePath = c.storageDir + "/" + helpers.GetFilename(filename)
}

func (c *ContextImpl) GetComputeOutputFilename() string {
	return c.outputFilePath
}

/** Set reducer nodes so mapper knows where to send the data to */
func (c *ContextImpl) SetReducerNodes(reducers []*m.Node) {
	c.reducerNodes = reducers
}

/** Returns reducer so that Compute Engine can _shuffle_ the mapper output files */
func (c *ContextImpl) GetReducers() []*m.Node {
	return c.reducerNodes
}

/**
* Returns the filenames of the output files so Compute Engine can _shuffle_ them,
* the integer would be the partition index - referred to reducerNodes - that file goes to
 */
func (c *ContextImpl) GetMapperOutputFiles() map[string]int {
	return c.mapperOutputFiles
}

func (c *ContextImpl) GetStorageDir() string {
	return c.storageDir
}

func (c *ContextImpl) ClearKeysTracker() {
	c.keysTracker = nil
}

func (c *ContextImpl) Write(key, value []byte) {
	if len(key) == 0 || len(value) == 0 {
		return
	}
	/** determine where each line is going to be persisted, */
	/** due to partitioning depending on number of reducers */
	stringifiedKey := strings.TrimSpace(string(key))
	var path string
	var partitionIndex int
	if extantPath, prst := c.keysTracker[stringifiedKey]; !prst {
		numReducers := len(c.reducerNodes)
		if numReducers == 1 {
			partitionIndex = 0
		} else {
			hashinator := fnv.New32a()
			hashinator.Write([]byte(stringifiedKey))
			hash := int32(math.Abs(float64(hashinator.Sum32())))
			partitionIndex = int(hash % int32(numReducers))
		}
		/* path for reducers will have the form of <chunkName>-part-<partitionIndex> */
		path = c.outputFilePath + "-part-" + strconv.Itoa(partitionIndex)
		c.keysTracker[stringifiedKey] = path

		/* create path to partition file */
		if _, err := os.Stat(path); err == nil {
			/* path exists - do nothing */
		} else if errors.Is(err, os.ErrNotExist) { // path does *not* exist
			helpers.CreatePaths(c.storageDir)
			_, err := os.Create(path)
			if err != nil {
				return
			}
		}
	} else {
		path = extantPath
	}
	if path == "" {
		logrus.Error("no path for: " + stringifiedKey)
	}
	if _, prst := c.mapperOutputFiles[path]; !prst {
		c.mapperOutputFiles[path] = partitionIndex
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logrus.WithFields(logrus.Fields{"Filename": path}).Info("Error closing file")
		}
	}(file)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("Error opening temp compute file")
		return
	}
	/* line is the composition of Key and a Value separated by a line */
	var line []byte
	line = append(key, []byte("\t")...)
	line = append(line, value...)
	line = append(line, []byte("\n")...)
	_, err = file.Write(line)
	if err != nil {
		return
	}
	_ = file.Sync()
	if err != nil {
		return
	}
}

/* Reducer emits an output file which will then be stored in the DFS */
func (c *ContextImpl) Emit(key, value []byte) {
	path := c.outputFilePath
	if _, err := os.Stat(path); err == nil {
		// path exists - do nothing
	} else if errors.Is(err, os.ErrNotExist) { // path does *not* exist
		helpers.CreatePaths(c.storageDir)
		_, err := os.Create(path)
		if err != nil {
			return
		}
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("Error opening reduce temp compute file")
		return
	}
	/** line is the composition of Key and a Value separated by a line */
	var line []byte
	line = append(key, []byte("\t")...)
	line = append(line, value...)
	// line = append(line, []byte("\n")...)
	file.Write(line)
	file.Sync()
	file.Close()
}
