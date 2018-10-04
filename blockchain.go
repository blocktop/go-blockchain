// Copyright © 2018 J. Strobus White.
// This file is part of the blocktop blockchain development kit.
//
// Blocktop is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Blocktop is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with blocktop. If not, see <http://www.gnu.org/licenses/>.

package blockchain

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	push "github.com/blocktop/go-push-components"
	spec "github.com/blocktop/go-spec"
	"github.com/fatih/color"
	"github.com/golang/glog"
	"github.com/spf13/viper"
)

type Blockchain struct {
	sync.Mutex
	blockchainType      string
	broadcast           spec.NetworkBroadcaster
	blockGenerator      spec.BlockGenerator
	consensus           spec.Consensus
	started             bool
	awaiting            *sync.Map
	peerID              string
	logPeerID           string
	confirmQ            *push.PushQueue
	confirmLocalQ       *push.PushQueue
	generateBlockNumber uint64
	confirmBlockNumber  uint64
	intervalStop        chan bool
}

var bold *color.Color = color.New(color.Bold)

func NewBlockchain(blockGenerator spec.BlockGenerator, consensus spec.Consensus, peerID string) *Blockchain {
	b := &Blockchain{}

	b.blockchainType = viper.GetString("blockchain.type")
	b.blockGenerator = blockGenerator
	b.consensus = consensus
	b.awaiting = &sync.Map{}
	b.intervalStop = make(chan bool, 1)

	b.confirmQ = push.NewPushQueue(1, 100, func(item push.QueueItem) {
		if block, ok := b.castToBlock(item); ok {
			b.confirmBlock(block, false)
		}
	})
	b.confirmQ.OnFirstOverload(func(item push.QueueItem) {
		glog.Errorln(color.HiRedString("Peer %s: confirm queue was overloaded", b.logPeerID))
	})

	b.confirmLocalQ = push.NewPushQueue(1, 100, func(item push.QueueItem) {
		if block, ok := b.castToBlock(item); ok {
			b.confirmBlock(block, true)
		}
	})
	b.confirmLocalQ.OnFirstOverload(func(item push.QueueItem) {
		glog.Errorln(color.HiRedString("Peer %s: confirm local queue was overloaded", b.logPeerID))
	})

	b.confirmQ.Start()
	b.confirmLocalQ.Start()

	b.peerID = peerID
	if peerID[:2] == "Qm" {
		b.logPeerID = peerID[2:8] // remove the "Qm" and take 6 runes
	} else {
		b.logPeerID = peerID[:6]
	}

	return b
}

func (b *Blockchain) castToBlock(item push.QueueItem) (spec.Block, bool) {
	block, ok := item.(spec.Block)
	if !ok {
		glog.Warningf("Peer %s: received wrong item type in block confirm queue", b.logPeerID)
	}
	return block, ok
}

func (b *Blockchain) Type() string {
	return b.blockchainType
}

func (b *Blockchain) BlockGenerator() spec.BlockGenerator {
	return b.blockGenerator
}

func (b *Blockchain) Consensus() spec.Consensus {
	return b.consensus
}

func (b *Blockchain) Start(ctx context.Context, broadcastFn spec.NetworkBroadcaster) {
	if b.started {
		return
	}
	b.started = true

	fmt.Fprintf(os.Stderr, "Peer %s: Starting %s\n", b.logPeerID, b.Type())

	b.broadcast = broadcastFn

	b.consensus.OnBlockConfirmed(func(block spec.Block) {
		b.confirmQ.Put(block)
	})
	b.consensus.OnLocalBlockConfirmed(func(block spec.Block) {
		b.confirmLocalQ.Put(block)
	})

	go b.runBlockInterval(ctx)

	if viper.GetBool("blockchain.genesis") {
		go b.generateGenesis()
	}
}

func (b *Blockchain) runBlockInterval(ctx context.Context) {
	ticker := time.NewTicker(viper.GetDuration("blockchain.blockInterval"))
	for {
		select {
		case <-ctx.Done():
		case <-b.intervalStop:
			return
		case <-ticker.C:
			// Since blocks are arriving much more frequently than this
			// single node is producing them, we expect that there will
			// always be some branch ready to compete on. However, we
			// check just in case there is not.
			branch, switchHeads := b.consensus.Competition().Branch(b.generateBlockNumber)
			// TODO: if switchHeads find latest block generated on the new head, if any.
			// Generate on that.
			_ = switchHeads
			if branch != nil {
				b.generateBlockNumber = branch[0].BlockNumber() + 1
				go b.generateBlock(branch)
			} else {
				glog.V(2).Infof("Peer %s: %s had no competition at block %d", b.logPeerID, b.blockchainType, b.generateBlockNumber)
			}
		}
	}
}

func (b *Blockchain) Stop() {
	if !b.started {
		return
	}

	fmt.Fprintf(os.Stderr, "\nPeer %s: Stopping %s", b.logPeerID, b.Type())

	//TODO block until queues are drained
	b.started = false
}

func (b *Blockchain) IsRunning() bool {
	return b.started
}

func (b *Blockchain) ReceiveBlock(netMsg *spec.NetworkMessage) error {
	block, err := b.blockGenerator.ReceiveBlock(netMsg)
	if err != nil {
		return err
	}

	blockID := block.Hash()

	if blockID == "" {
		glog.Errorln(color.HiRedString("Peer %s: bad block received from %s", netMsg.From[2:6]))
		return nil
	}

	if b.consensus.WasSeen(block) {
		return nil
	}

	if _, loaded := b.awaiting.LoadOrStore(blockID, true); loaded {
		return nil
	}

	glog.V(1).Infof("Peer %s: %s received block %d:%s", b.logPeerID, b.blockchainType, block.BlockNumber(), blockID[:6])

	if !block.Validate() {
		glog.V(1).Infof("Peer %s: %s received invalid block %d:%s", b.logPeerID, b.Type(), block.BlockNumber(), blockID[:6])
		return nil
	}

	if b.consensus.AddBlock(block, false) {
		glog.V(1).Infof("Peer %s: %s added and broadcasting block %d:%s", b.logPeerID, b.Type(), block.BlockNumber(), blockID[:6])
		b.broadcast(netMsg)
	} else {
		glog.V(1).Infof("Peer %s: %s disqualified block %d:%s", b.logPeerID, b.Type(), block.BlockNumber(), blockID[:6])
	}

	b.awaiting.Delete(blockID)
	return nil
}

func (b *Blockchain) ReceiveTransaction(netMsg *spec.NetworkMessage) error {
	_, err := b.blockGenerator.ReceiveTransaction(netMsg)
	if err != nil {
		return err
	}
	return nil
}

func (b *Blockchain) generateGenesis() {
	glog.Warningf("Peer %s: %s generating genesis block", b.logPeerID, b.Type())
	newBlock := b.blockGenerator.GenerateGenesisBlock()
	b.processNewBlock(newBlock)
	b.generateBlockNumber++
}

func (b *Blockchain) generateBlock(branch []spec.Block) {
	hd := make([]string, 4)
	for i, bl := range branch {
		if i > 3 {
			break
		}
		hd[i] = bl.Hash()[:6]
		if i == 0 {
			hd[i] = bold.Sprint(hd[i])
		}
	}
	head := branch[0]
	glog.Warningf("Peer %s: %s generating block %d for branch %v", b.logPeerID, b.Type(), head.BlockNumber()+1, hd)
	b.consensus.SetCompeted(head)
	newBlock := b.blockGenerator.GenerateBlock(branch)

	b.processNewBlock(newBlock)
}

func (b *Blockchain) processNewBlock(newBlock spec.Block) {
	if newBlock == nil {
		return
	}

	if !newBlock.Validate() {
		glog.V(1).Infof("Peer %s: %s generated invalid block:\n %v", b.logPeerID, b.Type(), newBlock)
		return
	}

	if !b.consensus.AddBlock(newBlock, true) {
		glog.V(1).Infof("Peer %s: %s disqualified local block %s", b.logPeerID, b.Type(), newBlock.Hash()[:6])
		return
	}

	glog.V(1).Infof("Peer %s: %s generated block %d:%s", b.logPeerID, b.Type(), newBlock.BlockNumber(), newBlock.Hash()[:6])

	p := &spec.MessageProtocol{}
	p.SetBlockchainType(b.blockchainType)
	p.SetResourceType(spec.ResourceTypeBlock)
	p.SetComponentType(newBlock.Name())
	p.SetVersion(newBlock.Version())

	data, links, err := newBlock.Marshal()
	if err != nil {
		glog.Errorln(color.HiRedString("Peer %s: generated bad block %s", b.logPeerID, newBlock.Hash()))
		return
	}
	netMsg := &spec.NetworkMessage{
		Data:     data,
		Links:    links,
		Hash:     newBlock.Hash(),
		From:     b.peerID,
		Protocol: p}

	go b.broadcast(netMsg)
}

func (b *Blockchain) confirmBlock(block spec.Block, local bool) {
	if b.confirmBlockNumber > 0 && block.BlockNumber() != b.confirmBlockNumber+1 {
		panic(fmt.Sprintf("blocked attempt to confirm out of order: block %d", block.BlockNumber()))
	}
	b.confirmBlockNumber = block.BlockNumber()
	b.blockGenerator.CommitBlock(block)
	if local {
		glog.Warningf("Peer %s: %s local block %d confirmed %s", b.logPeerID, b.blockchainType, block.BlockNumber(), color.HiGreenString(bold.Sprint(block.Hash()[:6])))
	} else {
		glog.Warningf("Peer %s: %s block %d confirmed %s", b.logPeerID, b.blockchainType, block.BlockNumber(), bold.Sprint(block.Hash()[:6]))
	}
}
