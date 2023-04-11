package client

import (
	"adfs/helpers"
	m "adfs/messages"
	"os"
	"sort"
	"strings"
)

func prependBackArrowEmoji(s string) string {
	return "↩️ " + s
}

func prependFolderEmoji(s string) string {
	return "🗀 " + s + "/"
}

func prependFileEmoji(s string) string {
	return "🗎 " + s
}

func getItemsAt(path string) []*Item {
	files := make([]*Item, 0)
	entries := helpers.GetDirEntries(path)
	// add dirs first
	for _, f := range entries {
		if f.IsDir() {
			if string(f.Name()[0]) == "." {
				continue // don't want to show dotfiles
			}
			files = append(files, &Item{
				displayName: prependFolderEmoji(f.Name()),
				name:        f.Name(),
				isDir:       true,
			})
		}
	}
	// add files second
	for _, f := range entries {
		if !f.IsDir() {
			files = append(files, &Item{
				displayName: prependFileEmoji(f.Name()),
				name:        f.Name(),
				isDir:       false,
			})
		}
	}
	return files
}

func getItemFromDisplayName(items []*Item, s string) *Item {
	for _, v := range items {
		if v.displayName == s {
			return v
		}
	}
	panic("Something went wrong retrieving item from display name")
}

func getItemsDisplayNames(items []*Item) []string {
	i := make([]string, 0)
	for _, v := range items {
		i = append(i, v.displayName)
	}
	return i
}

func dirUp(path string) string {
	if path == PREV_FOLDER {
		return path
	}
	lastSlash := -1
	for i, char := range path {
		if string(char) == "/" {
			lastSlash = i
		}
	}
	s := ""
	for i, char := range path {
		if i < lastSlash {
			s += string(char)
		}
	}
	return s
}

func getFileSize(filename string) int {
	fi, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return int(fi.Size())
}

func getRemoteChoicesFor(dirname string, files []*m.File) []*Item {
	if dirname != "/" {
		dirname += "/"
	}
	dirAdded := make(map[string]bool)
	choices := []*Item{}
	// add dirs first
	for _, f := range files {
		localPath := strings.Split(dirname, "/")
		remotePath := strings.Split(f.Dirname, "/")
		if strings.Contains(f.Dirname, dirname) &&
			len(remotePath) > len(localPath) { // is subdir
			foldername := remotePath[len(localPath)-1]
			if dirAdded[foldername] {
				continue
			}
			item := &Item{
				displayName: prependFolderEmoji(foldername),
				name:        foldername,
				isDir:       true,
			}
			choices = append(choices, item)
			dirAdded[foldername] = true
		}
	}
	sort.Slice(choices, func(i, j int) bool {
		return strings.Compare(choices[i].name, choices[j].name) < 0
	})

	sortedFiles := []*Item{}
	// add files second
	for _, f := range files {
		localPaths := strings.Split(dirname, "/")
		remotePaths := strings.Split(f.Dirname, "/")
		if strings.Contains(f.Dirname, dirname) &&
			len(remotePaths) == len(localPaths) { // is file
			item := &Item{
				displayName: prependFileEmoji(helpers.GetFilename(f.Dirname)),
				name:        helpers.GetFilename(f.Dirname),
				isDir:       false,
			}
			sortedFiles = append(sortedFiles, item)
		}
	}
	sort.Slice(sortedFiles, func(i, j int) bool {
		return strings.Compare(sortedFiles[i].name, sortedFiles[j].name) < 0
	})
	choices = append(choices, sortedFiles...)
	return choices
}
