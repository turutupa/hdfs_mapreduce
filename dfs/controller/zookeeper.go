package controller

import (
	m "adfs/messages"
	"adfs/storageNode"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const ONLINE = "online"
const OFFLINE = "offline"
const SHUTDOWN = "stopWorker"
const FAILURE_DETECTOR_DELAY_S = 5

type Zookeeper interface {
	Start()
	Stop()
	GetNodes() []*ZNode
	Heartbeat(uuid string, stats *m.Stats)
	FailureDetector()
	StopFailureDetector()
	RegisterNode(node *m.Node)
	NodeDown(uuid string)
	AddListenerOnNodeDown(onNodeDown func(nodeUuid string))
}

type ZookeeperImpl struct {
	heartbeats          map[string]*ZNode
	nodeStatusCh        chan *ZNode
	zookeeperStatusCh   chan string
	heartbeatsDone      chan bool
	heartbeatScheduler  *time.Ticker
	onNodeDownListeners []func(nodeUuid string)
}

type ZNode struct {
	Uuid     string
	Time     time.Time
	Status   string
	Hostname string
	Port     int
	Stats    *m.Stats
}

func NewZookeeper() Zookeeper {
	return &ZookeeperImpl{
		heartbeats:          make(map[string]*ZNode),
		nodeStatusCh:        make(chan *ZNode),
		zookeeperStatusCh:   make(chan string),
		onNodeDownListeners: make([]func(nodeUuid string), 0),
	}
}

func (z *ZookeeperImpl) Start() {
	z.init()
	z.FailureDetector()
	go z.worker()
}

func (z *ZookeeperImpl) init() {
	if z.zookeeperStatusCh == nil {
		z.zookeeperStatusCh = make(chan string)
	}
	if z.nodeStatusCh == nil {
		z.nodeStatusCh = make(chan *ZNode)
	}
	if z.heartbeats == nil {
		z.heartbeats = make(map[string]*ZNode)
	}
	if z.onNodeDownListeners == nil {
		z.onNodeDownListeners = make([]func(nodeUuid string), 0)
	}
}

func (z *ZookeeperImpl) Stop() {
	z.StopFailureDetector()
	z.stopWorker()
}

func (z *ZookeeperImpl) worker() {
	for {
		select {
		case <-z.heartbeatsDone:
			return
		case <-z.heartbeatScheduler.C:
			z.checkHeartbeats()
		case nodeStatus := <-z.nodeStatusCh:
			z.handleNodeStatus(nodeStatus)
		case quit := <-z.zookeeperStatusCh:
			logrus.Info("Attempt close zookeeper")
			if z.closeZookeeper(quit) {
				return // shutting down zookeeper gracefully
			}
		}
	}
}

func (z *ZookeeperImpl) stopWorker() {
	z.zookeeperStatusCh <- SHUTDOWN
}

func (z *ZookeeperImpl) checkHeartbeats() {
	logrus.Info("Checking for failed storage nodes...")
	for uuid, node := range z.heartbeats {
		diff := time.Now().Sub(node.Time)
		if diff.Seconds() > storageNode.HEARTBEAT_DELAY_S*2 {
			delete(z.heartbeats, uuid)
			for _, f := range z.onNodeDownListeners {
				f(uuid)
			}
			logrus.WithFields(logrus.Fields{
				"UUID": uuid,
			}).Error("ZNode is DOWN!")
		}
	}
}

func (z *ZookeeperImpl) closeZookeeper(status string) bool {
	if status == SHUTDOWN {
		logrus.Info("Zookeeper shutting down.")
		return true
	}
	return false
}

func (z *ZookeeperImpl) handleNodeStatus(node *ZNode) {
	uuid := node.Uuid
	status := node.Status
	if status == ONLINE {
		n, present := z.heartbeats[uuid]
		if present {
			// heartbeat!
			n.Time = node.Time
			logrus.WithFields(logrus.Fields{
				"UUID": strings.TrimSpace(uuid),
			}).Info("Heartbeat")
			if node.Stats != nil {
				n.Stats = node.Stats
			}
		} else {
			// registering!
			z.heartbeats[uuid] = node
			logrus.WithFields(logrus.Fields{
				"UUID": strings.TrimSpace(uuid),
			}).Info("Registered Online ZNode")
		}
	} else if status == OFFLINE {
		// StorageIO ZNode is down
		delete(z.heartbeats, uuid)
		logrus.WithFields(logrus.Fields{
			"UUID": uuid,
		}).Info("StorageIO ZNode down")
	} else {
		logrus.Error("Unexpected node status. Expected <online/offline>")
	}
}

func (z *ZookeeperImpl) updateNodeStatus(nodeStatus *ZNode) {
	z.nodeStatusCh <- nodeStatus
}

func (z *ZookeeperImpl) GetNodes() []*ZNode {
	nodes := make([]*ZNode, 0)
	for _, node := range z.heartbeats {
		nodes = append(nodes, node)
	}
	return nodes
}

func (z *ZookeeperImpl) NodeDown(uuid string) {
	nodeStatus := &ZNode{
		Uuid:   uuid,
		Status: OFFLINE,
	}
	z.updateNodeStatus(nodeStatus)
}

func (z *ZookeeperImpl) RegisterNode(node *m.Node) {
	nodeStatus := &ZNode{
		Uuid:     node.Uuid,
		Hostname: node.Hostname,
		Port:     int(node.Port),
		Status:   ONLINE,
		Time:     time.Now(),
		Stats: &m.Stats{
			Downloaded: 0,
			Uploaded:   0,
			Replicated: 0,
			FreeSpace:  0,
		},
	}
	z.updateNodeStatus(nodeStatus)
}

func (z *ZookeeperImpl) Heartbeat(uuid string, stats *m.Stats) {
	nodeStatus := &ZNode{
		Uuid:   uuid,
		Status: ONLINE,
		Time:   time.Now(),
		Stats:  stats,
	}
	z.updateNodeStatus(nodeStatus)
}

func (z *ZookeeperImpl) FailureDetector() {
	z.heartbeatScheduler = time.NewTicker(FAILURE_DETECTOR_DELAY_S * time.Second)
	z.heartbeatsDone = make(chan bool)
}

func (z *ZookeeperImpl) StopFailureDetector() {
	z.heartbeatScheduler.Stop()
	z.heartbeatsDone <- true
}

func (z *ZookeeperImpl) AddListenerOnNodeDown(onNodeDown func(nodeUuid string)) {
	z.onNodeDownListeners = append(z.onNodeDownListeners, onNodeDown)
}
