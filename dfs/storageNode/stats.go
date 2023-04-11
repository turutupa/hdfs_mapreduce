package storageNode

import (
	m "adfs/messages"
	"os"

	"golang.org/x/sys/unix"
)

const ADD_DOWNLOADED = "add_downloaded"
const ADD_UPLOADED = "add_uploaded"
const ADD_REPLICATED = "add_replicated"

type StatsBoard interface {
	Start()
	Stop()
	GetDownloaded() int
	AddDownloaded()
	GetUploaded() int
	AddUploaded()
	GetReplicated() int
	AddReplicated()
	GetFreeSpace() int
	GetAll() *m.Stats
}

type StatsBoardImpl struct {
	downloaded int
	uploaded   int
	replicated int
	freeSpace  int64
	updatesCh  chan string
	quit       chan bool
}

func NewStatsBoard() StatsBoard {
	return &StatsBoardImpl{
		updatesCh: make(chan string),
		quit:      make(chan bool),
	}
}

func (sm *StatsBoardImpl) Start() {
	go sm.worker()
}

func (sm *StatsBoardImpl) Stop() {
	sm.quit <- true
}

func (sm *StatsBoardImpl) GetDownloaded() int {
	return sm.downloaded
}

func (sm *StatsBoardImpl) AddDownloaded() {
	sm.updatesCh <- ADD_DOWNLOADED
}

func (sm *StatsBoardImpl) GetUploaded() int {
	return sm.uploaded
}

func (sm *StatsBoardImpl) AddUploaded() {
	sm.updatesCh <- ADD_UPLOADED
}

func (sm *StatsBoardImpl) GetReplicated() int {
	return sm.replicated
}

func (sm *StatsBoardImpl) AddReplicated() {
	sm.updatesCh <- ADD_REPLICATED
}

func (sm *StatsBoardImpl) GetFreeSpace() int {
	var stat unix.Statfs_t
	wd, _ := os.Getwd()
	unix.Statfs(wd, &stat)
	toGb := 1 << 30
	// Available blocks * size per block = available space in bytes
	return int((stat.Bavail * uint64(stat.Bsize)) / uint64(toGb))
}
func (sm *StatsBoardImpl) GetAll() *m.Stats {
	return &m.Stats{
		Downloaded: int32(sm.downloaded),
		Uploaded:   int32(sm.uploaded),
		Replicated: int32(sm.replicated),
		FreeSpace:  int32(sm.GetFreeSpace()),
	}
}

func (sm *StatsBoardImpl) worker() {
	for {
		select {
		case <-sm.quit:
			return
		case increase := <-sm.updatesCh:
			switch increase {
			case ADD_DOWNLOADED:
				sm.downloaded++
			case ADD_UPLOADED:
				sm.uploaded++
			case ADD_REPLICATED:
				sm.replicated++
			}
		}
	}
}
