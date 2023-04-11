package extsort

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// max number of lines kept in memory
const NUM_LINES_IN_MEMORY = 1000

type ExtSort interface {
	Sort(filePath string) error
}

type ExtSortImpl struct {
}

func NewExtSort() ExtSort {
	return &ExtSortImpl{}
}

func (es *ExtSortImpl) Sort(filePath string) error {
	tempFilePath := filePath + "-sorting"
	/** copy file as temporal and delete original */
	err := copyFile(filePath, tempFilePath)
	os.Remove(filePath)
	if err != nil {
		return err
	}

	file, err := os.Open(tempFilePath)
	if err != nil {
		return err
	}

	/** divide into sorted chunks/files */
	lines := []Pair{}
	i := 0
	tempChunkPaths := []string{}
	scanner := bufio.NewScanner(file)
	for {
		hasNextLine := scanner.Scan()
		if i == NUM_LINES_IN_MEMORY || !hasNextLine {
			sort.Slice(lines[:], func(i, j int) bool {
				return lines[i].key < lines[j].key
			})
			tempChunkPath := tempFilePath + "-" + strconv.Itoa(len(tempChunkPaths))
			os.Create(tempChunkPath)
			tempChunk, err := os.OpenFile(tempChunkPath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				return err
			}
			tempChunkPaths = append(tempChunkPaths, tempChunkPath)
			for _, line := range lines {
				l := line.key + "\t" + line.value + "\n"
				tempChunk.Write([]byte(l))
			}
			lines = []Pair{}
			i = 0
		} else {
			line := scanner.Text()
			if line != "" {
				lines = append(lines, getPair(line))
			}
			i++
		}

		if !hasNextLine {
			break
		}
	}
	file.Close()

	/** merge sorted chunks */
	/* Maps a mapper output file scanner to the next key in the file */
	mergeMap := make(map[*bufio.Scanner]string)
	files := []*os.File{}
	/* Create a scanner for each mapper output file and read the first line */
	for _, filename := range tempChunkPaths {
		file, err := os.Open(filename)
		files = append(files, file)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(file)
		scanner.Scan()
		mergeMap[scanner] = scanner.Text()
	}

	// What should we name the intermediary file where the merge results will go? Where should we store this file?
	mergedFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	/* Merge the Mapper output files into a single file */
	for len(mergeMap) != 0 {
		var smallestScanner *bufio.Scanner
		var smallestText string
		for scanner, text := range mergeMap {
			if smallestText == "" || strings.Compare(text, smallestText) <= 0 {
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
	mergedFile.Close()

	/** close files */
	for _, file := range files {
		file.Close()
	}

	/** remove temp files */
	for _, chunkPath := range tempChunkPaths {
		os.Remove(chunkPath)
	}
	os.Remove(tempFilePath)

	return nil
}

type Pair struct {
	key   string
	value string
}

func getPair(line string) Pair {
	i := 0
	for j, x := range line {
		if string(x) == "\t" {
			i = j
			return Pair{line[:i], line[i+1:]}
		}
	}
	return Pair{line[:i], line[i+1:]}
}

func copyFile(filePath, tempFilePath string) error {
	in, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	err = out.Sync()
	return nil
}
