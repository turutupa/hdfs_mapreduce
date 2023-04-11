package client

import (
	h "adfs/helpers"
	m "adfs/messages"
	"fmt"
	"strconv"
	"time"
)

func printClusterInformation(storageNodes []*m.Node) {
	for _, sn := range storageNodes {
		stats := sn.Stats
		fmt.Println("")
		fmt.Println("StorageNode UUID: " + sn.Uuid)
		fmt.Println("Transferred chunks........................" + strconv.Itoa(int(stats.Downloaded)))
		fmt.Println("Stored chunks............................." + strconv.Itoa(int(stats.Uploaded)))
		fmt.Println("Replicated chunks........................." + strconv.Itoa(int(stats.Replicated)))
		fmt.Println("Free space................................" + strconv.Itoa(int(stats.FreeSpace)) + " GB")
		fmt.Println("------------------------------------------------------")
	}
}

func dialog(msg string) {
	h.ClearTerminal()
	fmt.Println(msg)
	<-time.After((PRINT_TIME_S + 1) * time.Second)
	h.ClearTerminal()
}

func dialogAppend(msg ...string) {
	for _, m := range msg {
		fmt.Println(m)
	}
}

func centered(msg string) string {
	return "\n\n\t" + msg + "\n\n"
}

func success(msg string) string {
	return centered(msg + " ✔️")
}

func fail(msg string) string {
	return centered("❌" + msg + " ❌")
}
