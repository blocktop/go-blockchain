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
	receiveBlockQ       *push.PushQueue
	generateBlockNumber uint64
	confirmBlockNumber  uint64
	intervalStop        chan bool
	interval            *blockInterval
}

var bold *color.Color = color.New(color.Bold)

func NewBlockchain(blockGenerator spec.BlockGenerator, consensus spec.Consensus, peerID string) *Blockchain {
	b := &Blockchain{}

	b.blockchainType = viper.GetString("blockchain.type")
	b.blockGenerator = blockGenerator
	b.consensus = consensus
	b.awaiting = &sync.Map{}
	b.intervalStop = make(chan bool, 1)
	b.interval = newBlockInterval()

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

	b.receiveBlockQ = push.NewPushQueue(1, 1000, func(item push.QueueItem) {
		data := item.([]interface{})
		block := data[0].(spec.Block)
		netMsg := data[1].(*spec.NetworkMessage)

		b.receiveBlockWorker(block, netMsg)
	})
	// TODO: some kind of overload protection.

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

// 1. Evaluate heads
// 2a. Generate block   2b. StartComms
// 											3b. Receive blocks
// 4. Stop comms when block generate done
// 6. Confirm blocks

const (
	stateEval = iota
	stateGen
	stateConf
)

func (b *Blockchain) runBlockInterval(ctx context.Context) {
	state := stateEval
	var comp spec.Competition

	for {
		select {
		case <-ctx.Done():
		case <-b.intervalStop:
			return
		default:
			switch state {
			case stateEval:
				startTime := time.Now().UnixNano()
				comp = b.consensus.Competition()
				endTime := time.Now().UnixNano()
				b.interval.addEvalHeadTime(endTime - startTime)
				state = stateGen

			case stateGen:

				// Start receiving blocks
				b.receiveBlockQ.Start()

				// Start a timer for the portion of the block interval calculated
				// for network time.
				t := time.NewTimer(b.interval.getNetworkTime())

				// Get the branch we are to generate on.
				branch, rootID, switchHeads := comp.Branch(b.generateBlockNumber)
				// TODO: if switchHeads find latest block generated on the new head, if any.
				// Generate on that.
				_ = switchHeads
				var newBlock spec.Block
				if branch != nil {
					b.generateBlockNumber = branch[0].BlockNumber() + 1
					newBlock = b.generateBlock(branch, rootID)
				} else {
					glog.V(2).Infof("Peer %s: %s had no competition at block %d", b.logPeerID, b.blockchainType, b.generateBlockNumber)
				}

				// Wait for the block to generate and the timer to expire, which ever comes last.
				<-t.C

				// Stop receiving blocks.
				b.receiveBlockQ.Stop()

				// Add the new block to consensus.
				if newBlock != nil {
					startTime := time.Now().UnixNano()
					b.processNewBlock(newBlock)
					endTime := time.Now().UnixNano()
					b.interval.addAddBlockTime(endTime - startTime)
				}

				state = stateConf

			case stateConf:
				startTime := time.Now().UnixNano()
				b.consensus.ConfirmBlocks()
				endTime := time.Now().UnixNano()
				b.interval.addConfBlockTime(endTime - startTime)
				state = stateEval
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

	b.receiveBlockQ.Put([]interface{}{block, netMsg})

	return nil
}

func (b *Blockchain) receiveBlockWorker(block spec.Block, netMsg *spec.NetworkMessage) {
	if b.consensus.AddBlock(block, false) {
		glog.V(1).Infof("Peer %s: %s added and broadcasting block %d:%s", b.logPeerID, b.Type(), block.BlockNumber(), block.Hash()[:6])
		b.broadcast(netMsg)
	} else {
		glog.V(1).Infof("Peer %s: %s disqualified block %d:%s", b.logPeerID, b.Type(), block.BlockNumber(), block.Hash()[:6])
	}

	b.awaiting.Delete(block.Hash())
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
	b.generateBlockNumber = 0
}

func (b *Blockchain) generateBlock(branch []spec.Block, rootID int) spec.Block {
	branchLog := "[ "
	for i, bl := range branch {
		if i > 3 {
			break
		}
		blockID := bl.Hash()[:6]
		if i == 0 {
			blockID = bold.Sprint(blockID)
		}
		branchLog += fmt.Sprintf("%d:%s ", bl.BlockNumber(), blockID)
	}
	branchLog += fmt.Sprintf("... %d ]", rootID)
	head := branch[0]
	glog.Warningf("Peer %s: %s generating block %d for branch %s", b.logPeerID, b.Type(), head.BlockNumber()+1, branchLog)
	b.consensus.SetCompeted(head)
	newBlock := b.blockGenerator.GenerateBlock(branch)

	if newBlock == nil {
		return nil
	}

	if !newBlock.Validate() {
		glog.V(1).Infof("Peer %s: %s generated invalid block:\n %v", b.logPeerID, b.Type(), newBlock)
		return nil
	}

	return newBlock
}

func (b *Blockchain) processNewBlock(newBlock spec.Block) {
	if !b.consensus.AddBlock(newBlock, true) {
		glog.V(1).Infof("Peer %s: %s disqualified local block %d:%s", b.logPeerID, b.Type(), newBlock.BlockNumber(), newBlock.Hash()[:6])
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
