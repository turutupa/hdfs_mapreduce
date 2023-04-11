package helpers

import (
	"os"
)

func GetDirEntries(path string) []os.DirEntry {
	files, _ := os.ReadDir(path)
	return files
}

func GetFilename(path string) string {
	i := -1
	for j := 0; j < len(path); j++ {
		if string(path[j]) == "/" {
			i = j
		}
	}

	filename := ""
	for j := i + 1; j < len(path); j++ {
		filename += string(path[j])
	}
	return filename
}

func CreatePaths(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func GetPathFrom(filename string) string {
	i := 0
	for j := 0; j < len(filename); j++ {
		if string(filename[j]) == "/" {
			i = j
		}
	}
	path := ""
	for j := 0; j < i; j++ {
		path += string(filename[j])
	}
	return path
}
