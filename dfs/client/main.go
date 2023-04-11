package client

import h "adfs/helpers"

type Config struct {
	HomeDir        string
	ControllerHost string
	ControllerPort int
	StorageDir     string
}

/**
 * Client is the main and entry class. Client is formed by two main classes:
 * - Actions: allows the client to perform Ls, Rm, Put, Get Cluster information requests.
 *   This class contains the actual actions. No Cli rendering involved.
 * - CLI: this is the UI application for CLI. Handles all of the user interactions through CLI
 *	 and rendering the corresopnding results.
 */
func Init(config Config) {
	actions := NewActions(
		h.GetAddr(config.ControllerHost, config.ControllerPort),
		config.StorageDir,
	)
	cli := NewCli(
		config.HomeDir,
		config.StorageDir,
		actions.List,            // is passed to CLI so it can retrieve remote files
		actions.GetClusterStats, // is passed to CLI so it can render Cluster information
	)
	client := NewClient(cli, actions)
	client.Start()
}
