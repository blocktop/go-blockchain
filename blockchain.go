package blockchain

import (
	"sync"
	"time"

	spec "github.com/blckit/go-spec"
	"github.com/golang/glog"
)

type Blockchain struct {
	sync.Mutex
	Type                  string
	blockGenerator        spec.BlockGenerator
	conesensus            spec.Consensus
	onBlockGenerated      spec.BlockGeneratedHandler
	onBlockConfirmed      spec.BlockConfirmedHandler
	onLocalBlockConfirmed spec.BlockConfirmedHandler
	BlockInterval         int64 //milliseconds
	started               bool
	localBlocks           []spec.Block
	awaiting              []string
	genesis               bool
	peerID                string
}

const DefaultBlockInterval = int64(3 * time.Second / time.Millisecond)

func NewBlockchain(bcType string, blockGenerator spec.BlockGenerator, consensus spec.Consensus, peerID string) *Blockchain {
	b := &Blockchain{}

	b.Type = bcType
	b.BlockInterval = DefaultBlockInterval
	b.blockGenerator = blockGenerator
	b.conesensus = consensus
	b.localBlocks = make([]spec.Block, 0)
	b.peerID = peerID
	consensus.OnBlockConfirmed(func(block spec.Block) {
		b.confirmBlock(block)
	})
	return b
}

func (b *Blockchain) GetType() string {
	if b.Type != "" {
		return b.Type
	}
	return "blckit"
}

func (b *Blockchain) ProduceGenesisBlock() {
	b.genesis = true
}

func (b *Blockchain) GetBlockGenerator() spec.BlockGenerator {
	return b.blockGenerator
}

func (b *Blockchain) GetConsensus() spec.Consensus {
	return b.conesensus
}

func (b *Blockchain) Start() {
	if b.started {
		return
	}
	b.started = true

	glog.Infof("Starting blockchain %s", b.GetType())

	go b.mainLoop()
}

func (b *Blockchain) Stop() {
	if !b.started {
		return
	}

	glog.Infof("Stopping blockchain %s", b.GetType())

	b.started = false
}

func (b *Blockchain) IsRunning() bool {
	return b.started
}

// RecieveBlock receives a block from the network and adds it to the
// consensus system.
func (b *Blockchain) ReceiveBlock(block spec.Block) {
	if !b.conesensus.WasSeen(block) {
		go b.receiveBlock(block)
	}
}

func (b *Blockchain) OnBlockGenerated(f spec.BlockGeneratedHandler) {
	b.onBlockGenerated = f
}

func (b *Blockchain) OnBlockConfirmed(f spec.BlockConfirmedHandler) {
	b.onBlockConfirmed = f
}

func (b *Blockchain) OnLocalBlockConfirmed(f spec.BlockConfirmedHandler) {
	b.onLocalBlockConfirmed = f
}

func (b *Blockchain) mainLoop() {
	for b.started {
		time.Sleep(200 * time.Millisecond)
		var newBlock spec.Block
		if b.genesis {
			b.genesis = false
			newBlock = b.blockGenerator.ProduceGenesisBlock()
		} else {
			branch := b.conesensus.GetBestBranch()
			if branch == nil || len(branch) == 0 {
				glog.Infof("Peer %s: %s has no heads", b.peerID[:6], b.GetType())
				continue
			}

			head := branch[0]
			nowMilli := time.Now().UnixNano() / int64(time.Millisecond)
			latestTime := nowMilli - b.BlockInterval
			headTime := head.GetTimestamp()
			if headTime > latestTime {
				continue
			}
			hd := make([]string, len(branch))
			for i, bl := range branch {
				hd[i] = bl.GetID()[:6]
			}
			glog.Infof("Peer %s: %s generating block %d for branch %v", b.peerID[:6], b.GetType(), head.GetBlockNumber() + 1, hd)
			b.conesensus.SetCompeted(head)
			newBlock = b.blockGenerator.GenerateBlock(branch)
		}

		if newBlock == nil {
			continue
		}

		if !newBlock.Validate() {
			glog.Infof("Peer %s: %s generated invalid block:\n %v", b.peerID[:6], b.GetType(), newBlock)
			continue
		}

		if !b.conesensus.AddBlock(newBlock) {
			glog.Infof("Peer %s: %s disqualified local block %s", b.peerID[:6], b.GetType(), newBlock.GetID()[:6])
			continue
		}

		b.Lock()
		b.localBlocks = append(b.localBlocks, newBlock)
		b.Unlock()

		if b.onBlockGenerated != nil {
			go b.onBlockGenerated(newBlock)
		}
	}
}

func (b *Blockchain) receiveBlock(block spec.Block) {
	// check - lock - check pattern
	blockID := block.GetID()
	if b.isAwaiting(blockID) {
		b.logAwaiting(blockID)
		return
	}

	b.Lock()
	if b.isAwaiting(blockID) {
		b.Unlock()
		b.logAwaiting(blockID)
		return
	}
	b.awaiting = append(b.awaiting, blockID)
	b.Unlock()

	if !block.Validate() {
		glog.Infof("Peer %s: %s received invalid block %s", b.peerID[:6], b.GetType(), blockID[:6])
		return 
	}
	if b.conesensus.AddBlock(block) {
		glog.Infof("Peer %s: %s added block %s", b.peerID[:6], b.GetType(), blockID[:6])
	} else {
		glog.Infof("Peer %s: %s disqualified block %s", b.peerID[:6], b.GetType(), blockID[:6])
	}
	
	b.removeAwaiting(blockID)
	return
}

func (b *Blockchain) logAwaiting(blockID string) {
	glog.Infof("Peer %s: %s already awaiting block %s", b.peerID[:6], b.GetType(), blockID[:6])
}

func (b *Blockchain) isAwaiting(blockID string) bool {
	for _, id := range b.awaiting {
		if id == blockID {
			return true
		}
	}
	return false
}

func (b *Blockchain) removeAwaiting(blockID string) {
	b.Lock()
	for i, id := range b.awaiting {
		if id == blockID {
			b.awaiting = append(b.awaiting[:i], b.awaiting[i+1:]...)
		}
	}
	b.Unlock()
}

func (b *Blockchain) confirmBlock(block spec.Block) {
	// is it a locally generated block?
	// also remove any blocks that are as old or older than the confirmed block
	blockID := block.GetID()
	blockNumber := block.GetBlockNumber()
	newLocalBlocks := make([]spec.Block, 0)
	var isLocal bool

	b.Lock()
	for i := 0; i < len(b.localBlocks); i++ {
		localBlock := b.localBlocks[i]
		if localBlock.GetID() == blockID {
			isLocal = true
			if b.onLocalBlockConfirmed != nil {
				go b.onLocalBlockConfirmed(block)
			}
		}
		if localBlock.GetBlockNumber() > blockNumber {
			newLocalBlocks = append(newLocalBlocks, localBlock)
		}
	}
	b.localBlocks = newLocalBlocks
	b.Unlock()

	if !isLocal && b.onBlockConfirmed != nil {
		go b.onBlockConfirmed(block)	
	}
}
