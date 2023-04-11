# Project 2: Distributed Computation Engine

- [Project spec](https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-2.html)
- [Project retrospective](retrospective.md)

## Start cluster

`./start-cluster.sh`

## Stop cluster

`./stop-cluster.sh`

## Run client

`./start-client.sh`

## How to run a MapReduce job

- Write your MapReduce job in Go and build it with `go build`
- Start the client
- Upload the file with your data to the DFS
- Select the option to run computation in the client, select your data, your job, and choose an output file name
- The job output will be stored in the DFS. Use the client to download the output file after the job has ran
