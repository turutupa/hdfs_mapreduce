package compute_engine

import (
	extsort "adfs/external_sort"
	"adfs/helpers"
	"adfs/messages"
	"bufio"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type ComputeEngine interface {
	RunMapper(pluginDir, dataPath string) error
	RunReducer(pluginDir, dataPath string) error
	Shuffle() map[string]*messages.Node
}

type MapReduce interface {
	Map(lineNumber int, lineText string, context Context)
	Reduce(key, value []byte, context Context)
}

type ComputeEngineImpl struct {
	context Context
}

func NewComputeEngine(context Context) ComputeEngine {
	return &ComputeEngineImpl{context}
}

func (ce *ComputeEngineImpl) RunMapper(pluginPath, dataPath string) error {
	file, err := os.Open(dataPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error(), "File": dataPath}).Error("Error opening file")
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("Error closing mapper output file")
		}
	}(file)
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		/** Run external plugin and get stdout */
		value := scanner.Text()
		key := strconv.Itoa(i)
		args := []string{MAP, key, value}
		out, err := exec.Command(pluginPath, args...).Output()
		if err != nil {
			logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error(), "Line": i}).Error("Error executing plugin")
			return err
		}
		if string(out) != "" && string(out) != "\n" && string(out) != "\t" {
			lines := strings.Split(string(out), "\n")
			for _, line := range lines {
				key, value, ok := parse(line, "\t")
				if !ok || key == "" || value == "" {
					continue
				}
				ce.context.Write([]byte(key), []byte(value))
			}
		}
		i++
	}
	ce.context.ClearKeysTracker()

	if err := scanner.Err(); err != nil {
		return err
	}

	/** Sort mapper output files before */
	for filename := range ce.context.GetMapperOutputFiles() {
		logrus.WithFields(logrus.Fields{"Filename": filename}).Info("External Sort")
		err = extsort.NewExtSort().Sort(filename)
		if err != nil {
			logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("External Sort error")
			return err
		}
	}

	/** Remove unnecessary plugin/files */
	os.Remove(pluginPath)

	return nil
}

func (ce *ComputeEngineImpl) RunReducer(pluginPath, dataPath string) error {
	file, err := os.Open(dataPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{"File": dataPath}).Error("Error opening file in reducer")
		return err
	}
	defer file.Close()

	stat, _ := file.Stat()
	logrus.WithFields(logrus.Fields{
		"Name":  stat.Name(),
		"Size":  stat.Size(),
		"IsDir": stat.IsDir(),
	}).Warn("Sorted File")
	logrus.Warn("Running reducer on: " + dataPath)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		/** Run external plugin and get stdout */
		line := scanner.Text()
		lineSlice := strings.Split(line, "\t")
		key := lineSlice[0]
		value := ""
		for _, valueString := range lineSlice[1:] {
			value += valueString + "\t"
		}

		args := []string{REDUCE, key, value}
		out, err := exec.Command(pluginPath, args...).Output()
		if err != nil {
			return err
		}
		key, value, ok := parse(string(out), "\t")
		if !ok || key == "" || value == "" {
			continue
		}
		ce.context.Emit([]byte(key), []byte(value))
	}

	if err := scanner.Err(); err != nil {
		logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("Error reading sorted file in Compute Engine - Run Redcuer")
		return err
	}

	return nil
}

/** Sends each mapper output file to the corresponding partition. Once sent, removes the local files */
func (ce *ComputeEngineImpl) Shuffle() map[string]*messages.Node {
	filesTable := make(map[string]*messages.Node)
	logrus.WithFields(logrus.Fields{"MapperOutputFiles": ce.context.GetMapperOutputFiles()}).Info("Mapper output files")
	for filePath, partitionIndex := range ce.context.GetMapperOutputFiles() {
		file, _ := os.ReadFile(filePath)
		reducerNode := ce.context.GetReducers()[partitionIndex]
		addr := helpers.GetAddr(reducerNode.Hostname, int(reducerNode.Port))
		msgHandler, _ := messages.GetMessageHandlerFor(addr)
		err := msgHandler.SendComputeStore(helpers.GetFilename(filePath), file)
		logrus.WithFields(logrus.Fields{"Filename": filePath, "ReducerHostname": reducerNode.Hostname, "ReducerPort": reducerNode.Port}).Info("Shuffling")
		if err != nil {
			logrus.WithFields(logrus.Fields{"ErrorMsg": err.Error()}).Error("Error sending mapper output file to reducer")
			return nil
		}
		filesTable[helpers.GetFilename(filePath)] = reducerNode
		if ce.context.GetNodeUuid() != reducerNode.Uuid {
			logrus.WithFields(logrus.Fields{"File": filePath}).Info("Deleting temporary file")
			os.Remove(filePath)
		}
	}
	return filesTable
}

func parse(str, delimiter string) (string, string, bool) {
	var i int
	for j, r := range str {
		if string(r) == delimiter {
			i = j
		}
	}
	if i == 0 {
		return "", "", false
	}
	return str[:i], str[i+1:], true
}
