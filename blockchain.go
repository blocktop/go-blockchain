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

	spec "github.com/blocktop/go-spec"
	"github.com/golang/glog"
	"github.com/spf13/viper"
)

type Blockchain struct {
	sync.Mutex
	blockchainType string
	broadcast      chan<- *spec.NetworkMessage
	blockGenerator spec.BlockGenerator
	consensus      spec.Consensus
	started        bool
	awaiting       []string
	peerID         string
	logPeerID      string
	stopProc1      chan bool
	stopProc2      chan bool
	confirm				 chan spec.Block
	confirmLocal   chan spec.Block
	compete        chan []spec.Block
}

func NewBlockchain(blockGenerator spec.BlockGenerator, consensus spec.Consensus, peerID string) *Blockchain {
	b := &Blockchain{}

	b.blockchainType = viper.GetString("blockchain.type")
	b.blockGenerator = blockGenerator
	b.consensus = consensus
	b.confirm = make(chan spec.Block, 25)
	b.confirmLocal = make(chan spec.Block, 10)
	b.compete = make(chan []spec.Block, 1)

	b.peerID = peerID
	if peerID[:2] == "Qm" {
		b.logPeerID = peerID[2:8] // remove the "Qm" and take 6 runes
	} else {
		b.logPeerID = peerID[:6]
	}

	return b
}

func (b *Blockchain) GetType() string {
	return b.blockchainType
}

func (b *Blockchain) GetBlockGenerator() spec.BlockGenerator {
	return b.blockGenerator
}

func (b *Blockchain) GetConsensus() spec.Consensus {
	return b.consensus
}

func (b *Blockchain) Start(ctx context.Context, broadcastChan chan<- *spec.NetworkMessage) {
	if b.started {
		return
	}
	b.started = true

	fmt.Fprintf(os.Stderr, "Peer %s: Starting %s\n", b.logPeerID, b.GetType())

	b.stopProc1 = make(chan bool, 1)
	b.stopProc2 = make(chan bool, 1)
	b.broadcast = broadcastChan

	b.consensus.Start(ctx, b.confirm, b.confirmLocal, b.compete)
	go b.generateBlocks(ctx)
	go b.confirmBlocks(ctx)
}

func (b *Blockchain) Stop() {
	if !b.started {
		return
	}

	fmt.Fprintf(os.Stderr, "\nPeer %s: Stopping %s", b.logPeerID, b.GetType())

	b.started = false

	b.stopProc1 <- true
	b.stopProc2 <- true
}

func (b *Blockchain) IsRunning() bool {
	return b.started
}

func (b *Blockchain) ReceiveBlock(netMsg *spec.NetworkMessage) {
	msg := netMsg.Message
	block := b.blockGenerator.Unmarshal(msg)

	if b.consensus.WasSeen(block) {
		return
	}

	blockID := block.GetID()
	if !b.setAwaiting(blockID) {
		return
	}

	glog.V(1).Infof("Peer %s: %s received block %s", b.logPeerID, b.blockchainType, block.GetID()[:6])

	if !block.Validate() {
		glog.V(1).Infof("Peer %s: %s received invalid block %s", b.logPeerID, b.GetType(), blockID[:6])
		return
	}

	if b.consensus.AddBlock(block, false) {
		glog.V(1).Infof("Peer %s: %s added block %s", b.logPeerID, b.GetType(), blockID[:6])
		b.broadcast <- netMsg
	} else {
		glog.V(1).Infof("Peer %s: %s disqualified block %s", b.logPeerID, b.GetType(), blockID[:6])
	}

	b.removeAwaiting(blockID)
}

func (b *Blockchain) ReceiveTransaction(netMsg *spec.NetworkMessage) {
	b.blockGenerator.ReceiveTransaction(netMsg)
}

func (b *Blockchain) generateBlocks(ctx context.Context) {
	if viper.GetBool("blockchain.genesis") {
		b.generateGenesis()
	}

	for {
		select {
		case <-b.stopProc1:
		case <-ctx.Done():
			return
		case branch := <-b.compete:
			b.generateBlock(branch)
		}
	}
}

func (b *Blockchain) generateGenesis() {
	glog.Warningf("Peer %s: %s generating genesis block", b.logPeerID, b.GetType())
	newBlock := b.blockGenerator.GenerateGenesisBlock()
	b.processNewBlock(newBlock)
}

func (b *Blockchain) generateBlock(branch []spec.Block) {
	hd := make([]string, len(branch))
	for i, bl := range branch {
		hd[i] = bl.GetID()[:6]
	}
	head := branch[0]
	glog.Warningf("Peer %s: %s generating block %d for branch %v", b.logPeerID, b.GetType(), head.GetBlockNumber()+1, hd)
	b.consensus.SetCompeted(head)
	newBlock := b.blockGenerator.GenerateBlock(branch)

	b.processNewBlock(newBlock)

}

func (b *Blockchain) processNewBlock(newBlock spec.Block) {
	if newBlock == nil {
		return
	}

	if !newBlock.Validate() {
		glog.V(1).Infof("Peer %s: %s generated invalid block:\n %v", b.logPeerID, b.GetType(), newBlock)
		return
	}

	if !b.consensus.AddBlock(newBlock, true) {
		glog.V(1).Infof("Peer %s: %s disqualified local block %s", b.logPeerID, b.GetType(), newBlock.GetID()[:6])
		return
	}

	glog.V(1).Infof("Peer %s: %s generated block %d: %s", b.logPeerID, b.GetType(), newBlock.GetBlockNumber(), newBlock.GetID()[:6])

	p := &spec.MessageProtocol{}
	p.SetBlockchainType(b.blockchainType)
	p.SetResourceType(spec.ResourceTypeBlock)
	p.SetComponentType(newBlock.GetType())
	p.SetVersion(newBlock.GetVersion())

	netMsg := &spec.NetworkMessage{
		Message: newBlock.Marshal(),
		From: b.peerID,
		Protocol: p}

	b.broadcast <- netMsg
}

func (b *Blockchain) confirmBlocks(ctx context.Context) {
	for {
		select {
		case <-b.stopProc2:
		case <-ctx.Done():
			return
		case block := <-b.confirm:
			b.blockGenerator.CommitBlock(block) 
		case block := <-b.confirmLocal:
			b.blockGenerator.CommitBlock(block)
			glog.Warningf("Peer %s: %s local block %s confirmed, reward = %d", b.logPeerID, b.blockchainType, block.GetID()[:6], 1000000000)
		}
	}
}

func (b *Blockchain) setAwaiting(blockID string) bool {
	// check - lock - check pattern
	if b.isAwaiting(blockID) {
		return false
	}

	b.Lock()
	if b.isAwaiting(blockID) {
		b.Unlock()
		return false
	}
	b.awaiting = append(b.awaiting, blockID)
	b.Unlock()
	return true
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
