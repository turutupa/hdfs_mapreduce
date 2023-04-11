package client

import (
	"adfs/helpers"
	m "adfs/messages"
	"math"
	"os"
	"strings"

	pui "github.com/manifoldco/promptui"
	"github.com/sirupsen/logrus"
)

type Cli interface {
	Start() *UserAction
	Stop()
	Get(dir string) *UserAction // refactor: cursor pos should not be part of interface
	Put(dir string) *UserAction // refactor: cursor pos should not be part of interface
	Rm(dir string) *UserAction  // refactor: cursor pos should not be part of interface
	GetClusterStats() *UserAction
	Reset()
}

type CliImpl struct {
	label                 string
	choices               []string
	cursorPos             []int
	filePaths             []string
	homeDir               string
	storageDir            string
	ls                    func() ([]*m.File, error)
	getClusterInformation func() ([]*m.Node, error)
}

type UserAction struct {
	action         string
	remoteFilename string
	localFilename  string
	outputFilename string
}

type Item struct {
	displayName string
	name        string
	isDir       bool
}

func NewCli(
	homeDir string,
	storageDir string,
	ls func() ([]*m.File, error),
	getClusterInformation func() ([]*m.Node, error),
) Cli {
	return &CliImpl{
		homeDir:               homeDir,
		storageDir:            storageDir,
		cursorPos:             make([]int, 1),
		ls:                    ls,
		getClusterInformation: getClusterInformation,
	}
}

func (c *CliImpl) Start() *UserAction {
	helpers.ClearTerminal()
	helpers.PrintTitle(TITLE)
	c.Reset()
	label := "What do you want to do?"
	choices := []*Item{
		{displayName: DOWNLOAD_FILE},
		{displayName: UPLOAD_FILE},
		{displayName: DELETE_FILE},
		{displayName: COMPUTE_FILE},
		{displayName: GET_CLUSTER_STATS},
		{displayName: EXIT},
	}
	selected, _ := selectPrompt(label, choices, 0)
	switch selected.displayName {
	case DOWNLOAD_FILE:
		return c.Get("/")
	case UPLOAD_FILE:
		return c.Put(c.homeDir)
	case DELETE_FILE:
		return c.Rm("/")
	case COMPUTE_FILE:
		return c.Compute(c.homeDir)
	case GET_CLUSTER_STATS:
		return c.GetClusterStats()
	case EXIT:
		return &UserAction{action: EOT}
	}

	panic("Something went wrong [cli.Start]")
}

func (c *CliImpl) Stop() {
	// User can exit gracefully selecting Exit.
}

func (c *CliImpl) Reset() {
	c.filePaths = []string{}
	c.cursorPos = []int{}
}

// TODO: Refactor Get() and Put() to remove duplicate code
func (c *CliImpl) Get(dir string) *UserAction {
	label := "Select remote file to download"
	userAction := c.handleRemoteFiles(label, dir, 0)
	if userAction == nil {
		return c.Start()
	}
	userAction.action = DOWNLOAD_FILE
	saveAsLabel := "Save file as. Ex: /<f1>/<f2>/<filename>"
	saveAs := inputPrompt(saveAsLabel)
	if string(saveAs[0]) != "/" {
		saveAs = "/" + saveAs
	}
	userAction.localFilename = saveAs
	return userAction
}

func (c *CliImpl) Put(dirname string) *UserAction {
	label := "Select file to upload"
	userAction := c.handleLocalFiles(label, dirname, 0)
	if userAction == nil {
		return c.Start()
	}
	userAction.action = UPLOAD_FILE
	uploadAsLabel := "Filename. Ex: /<f1>/<f2>/<filename>"
	remoteFilename := inputPrompt(uploadAsLabel)
	if string(remoteFilename[0]) != "/" {
		remoteFilename = "/" + remoteFilename
	}
	userAction.remoteFilename = remoteFilename
	return userAction
}

func (c *CliImpl) Rm(dir string) *UserAction {
	label := "Select remote file to delete"
	userAction := c.handleRemoteFiles(label, dir, 0)
	if userAction == nil {
		return c.Start()
	}
	userAction.action = DELETE_FILE
	return userAction
}

func (c *CliImpl) Compute(homeDir string) *UserAction {
	targetFile := c.handleRemoteFiles("Select file to compute", "/", 0)
	if targetFile == nil {
		return c.Start()
	}
	c.filePaths = []string{}
	selectJobLabel := "Select compute file"
	jobFile := c.handleLocalFiles(selectJobLabel, homeDir, 0)
	saveAsLabel := "Save output file as. Ex: /<f1>/<f2>/<filename>"
	saveAs := inputPrompt(saveAsLabel)
	if string(saveAs[0]) != "/" {
		saveAs = "/" + saveAs
	}
	return &UserAction{
		action:         COMPUTE_FILE,
		remoteFilename: targetFile.remoteFilename,
		localFilename:  jobFile.localFilename,
		outputFilename: saveAs,
	}
}

func (c *CliImpl) GetClusterStats() *UserAction {
	label := "Cluster Summary"
	if storageNodes, err := c.getClusterInformation(); err != nil {
		dialog(fail(err.Error()))
	} else {
		printClusterInformation(storageNodes)
	}
	choices := []*Item{{
		displayName: MAIN_MENU,
	}}
	selectPrompt(label, choices, 0)

	return c.Start()
}

/**
* Renders files of distributed file system. Distributed file system will give
* all the files as a list, and each file will have a name in the form of:
* $ /folder1/folder2/filename
* therefore, we need to only render the files for "/"
* if user selects folder1 then it should only render files
* inside "folder1", and so on
 */
func (c *CliImpl) handleRemoteFiles(
	label string,
	dirname string,
	cursorPos int,
) *UserAction {
	if dirname == "" {
		dirname = "/"
	}
	remoteFiles, err := c.ls()
	if err != nil {
		dialog(fail("A-DFS is not online"))
		return nil
	}
	choices := getRemoteChoicesFor(dirname, remoteFiles)
	choices = setChoices(choices)
	selected, pos := selectPrompt(label, choices, cursorPos)

	if selected.name == PREV_FOLDER {
		if len(c.filePaths) == 0 {
			return nil
		} else {
			newPath, _ := pop(c.filePaths)
			c.filePaths = newPath // remove last dir
			newCursors, popped := pop(c.cursorPos)
			c.cursorPos = newCursors // remove last cursor
			return c.handleRemoteFiles(label, dirUp(dirname), popped)
		}
	} else if selected.name == MAIN_MENU {
		return nil
	} else if selected.isDir {
		var newDir string
		if dirname == "/" {
			newDir = dirname + selected.name
		} else {
			newDir = dirname + "/" + selected.name
		}
		c.filePaths = append(c.filePaths, selected.name)
		c.cursorPos = append(c.cursorPos, pos)
		return c.handleRemoteFiles(label, newDir, 0)
	} else {
		userAction := &UserAction{}
		remotePath := append(c.filePaths, selected.name)
		remoteDirname := strings.Join(remotePath, "/")
		userAction.remoteFilename = "/" + remoteDirname
		return userAction
	}
}

/*
* This function renders local file system. To do so it makes recursive calls
* internally to retrieve files and folders in path
 */
func (c *CliImpl) handleLocalFiles(label, dirname string, cursorPos int) *UserAction {
	choices := setChoices(getItemsAt(dirname))
	selected, pos := selectPrompt(label, choices, cursorPos)

	if selected.name == PREV_FOLDER {
		if dirname == c.homeDir {
			return nil
		} else {
			newFilepaths, _ := pop(c.filePaths)
			c.filePaths = newFilepaths
			newCursors, popped := pop(c.cursorPos)
			c.cursorPos = newCursors
			return c.handleLocalFiles(label, dirUp(dirname), popped)
		}
	} else if selected.name == MAIN_MENU {
		return nil
	} else if selected.isDir {
		newPath := dirname + "/" + selected.name
		c.filePaths = append(c.filePaths, selected.name)
		c.cursorPos = append(c.cursorPos, pos)
		return c.handleLocalFiles(label, newPath, 0)
	} else {
		// Put together file dirname
		userAction := &UserAction{}
		localPath := append(c.filePaths, selected.name)
		localFilename := strings.Join(localPath, "/")
		userAction.localFilename = c.homeDir + localFilename
		return userAction
	}
}

/**
* Adds Go to previous folder option and go to main menu option to
* a list of prompt choices
 */
func setChoices(c []*Item) []*Item {
	choices := []*Item{{
		displayName: prependBackArrowEmoji(PREV_FOLDER),
		name:        PREV_FOLDER,
		isDir:       true,
	}}
	choices = append(choices, c...)
	choices = append(choices, &Item{
		displayName: prependBackArrowEmoji(MAIN_MENU),
		name:        MAIN_MENU,
		isDir:       false,
	})
	return choices
}

func inputPrompt(label string) string {
	prompt := pui.Prompt{
		Label: label,
	}
	selected, err := prompt.Run()
	if err != nil {
		logrus.Error(err.Error())
		os.Exit(1)
	}
	if selected == "" {
		return inputPrompt(label)
	}
	return selected
}

func selectPrompt(label string, choices []*Item, cursorPos int) (*Item, int) {
	searcher := func(input string, i int) bool {
		choice := choices[i]
		name := strings.Replace(strings.ToLower(choice.name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)

		return strings.Contains(name, input)
	}

	prompt := pui.Select{
		Label:        label,
		Items:        getItemsDisplayNames(choices),
		Size:         20,
		CursorPos:    cursorPos,
		HideSelected: true,
		Searcher:     searcher,
	}

	zero := float64(0)
	scroll := int(math.Max(zero, float64(cursorPos-5)))
	// 5 is just a random number to make it look decent on terminal
	cursorPos, selected, err := prompt.RunCursorAt(cursorPos, scroll)
	if err != nil {
		logrus.Error(err.Error())
		os.Exit(1)
	}
	return getItemFromDisplayName(choices, selected), cursorPos
}

func pop[T comparable](s []T) ([]T, T) {
	if len(s) == 0 {
		var null T
		return s, null
	}
	n := len(s) - 1
	elem := s[n]
	s = s[:n]
	return s, elem
}
