package client

import (
	h "adfs/helpers"
	"fmt"
	"os"
)

const TITLE = "       A-DFS"
const EOT = "---END OF TRANSMISSION---"
const PRINT_TIME_S = 3
const CONNECTION_ERROR_MSG = "Oops! Seems like you are not connected to the Controller"

type Client interface {
	Start()
	Stop()
}

type ClientImpl struct {
	cli     Cli
	actions Actions
}

func NewClient(cli Cli, actions Actions) Client {
	return &ClientImpl{cli, actions}
}

func (c *ClientImpl) Start() {
	for {
		userAction := c.cli.Start()
		if userAction == nil {
			continue
		}
		localFilename := userAction.localFilename
		remoteFilename := userAction.remoteFilename
		if userAction.action == DOWNLOAD_FILE {
			c.actions.Download(localFilename, remoteFilename)
		} else if userAction.action == UPLOAD_FILE {
			c.actions.Upload(localFilename, remoteFilename)
		} else if userAction.action == DELETE_FILE {
			c.actions.Delete(remoteFilename)
		} else if userAction.action == COMPUTE_FILE {
			outputFilename := userAction.outputFilename
			c.actions.Compute(localFilename, remoteFilename, outputFilename)
		} else if userAction.action == EOT {
			c.Stop()
		}
		c.cli.Reset()
	}
}

func (c *ClientImpl) Stop() {
	h.ClearTerminal()
	h.PrintTitle(TITLE)
	farewellText := "---Farewell commander. Shutting down system---"
	farewellMsg := centered(farewellText)
	fmt.Println(farewellMsg)
	os.Exit(0)
}
