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

	"github.com/blocktop/go-kernel"

	push "github.com/blocktop/go-push-components"
	spec "github.com/blocktop/go-spec"
	"github.com/fatih/color"
	"github.com/golang/glog"
	"github.com/spf13/viper"
)

type Blockchain struct {
	sync.Mutex
	name               string
	blockGenerator     spec.BlockGenerator
	consensus          spec.Consensus
	started            bool
	awaiting           *sync.Map
	prID               string
	confirmQ           *push.PushQueue
	confirmLocalQ      *push.PushQueue
	confirmBlockNumber uint64
	intervalStop       chan bool
}

// Compile-time check for interface compliance
var _ spec.Blockchain = (*Blockchain)(nil)

var bold *color.Color = color.New(color.Bold)

func NewBlockchain(blockGenerator spec.BlockGenerator, consensus spec.Consensus) *Blockchain {
	b := &Blockchain{}

	b.name = viper.GetString("blockchain.name")
	b.blockGenerator = blockGenerator
	b.consensus = consensus
	b.awaiting = &sync.Map{}
	b.intervalStop = make(chan bool, 1)

	b.confirmQ = push.NewPushQueue(1, 100, func(item interface{}) {
		if block, ok := b.castToBlock(item); ok {
			b.confirmBlock(block, false)
		}
	})
	b.confirmQ.OnFirstOverload(func(item interface{}) {
		glog.Errorln(color.HiRedString("Confirm queue was overloaded"))
	})

	b.confirmLocalQ = push.NewPushQueue(1, 100, func(item interface{}) {
		if block, ok := b.castToBlock(item); ok {
			b.confirmBlock(block, true)
		}
	})
	b.confirmLocalQ.OnFirstOverload(func(item interface{}) {
		glog.Errorln(color.HiRedString("Confirm local queue was overloaded"))
	})

	b.confirmQ.Start()
	b.confirmLocalQ.Start()

	return b
}

func (b *Blockchain) peerID() string {
	if b.prID == "" {
		b.prID = kernel.Network().PeerID()
	}
	return b.prID
}

func (b *Blockchain) logPeerID() string {
	prID := b.peerID()
	if prID[:2] == "Qm" {
		return prID[2:8] // remove the "Qm" and take 6 runes
	}
	return prID[:6]
}

func (b *Blockchain) castToBlock(item interface{}) (spec.Block, bool) {
	block, ok := item.(spec.Block)
	if !ok {
		glog.Warningln("Received wrong item type in block confirm queue")
	}
	return block, ok
}

func (b *Blockchain) Name() string {
	return b.name
}

func (b *Blockchain) Consensus() spec.Consensus {
	return b.consensus
}

func (b *Blockchain) Start(ctx context.Context) {
	if b.started {
		return
	}
	b.started = true

	fmt.Fprintf(os.Stderr, "Starting %s\n", b.Name())

	b.consensus.OnBlockConfirmed(func(block spec.Block) {
		b.confirmQ.Put(block)
	})
	b.consensus.OnLocalBlockConfirmed(func(block spec.Block) {
		b.confirmLocalQ.Put(block)
	})
}

func (b *Blockchain) Stop() {
	if !b.started {
		return
	}

	fmt.Fprintf(os.Stderr, "\nStopping %s", b.Name())

	//TODO block until queues are drained
	b.started = false
}

func (b *Blockchain) IsRunning() bool {
	return b.started
}

func (b *Blockchain) AddBlocks(blocks []spec.Block, local bool) *spec.AddBlocksResponse {
	validBlocks := make([]spec.Block, 0)

	for _, block := range blocks {
		blockID := block.Hash()

		if blockID == "" {
			glog.Warningln(color.HiYellowString("ignoring bad block received"))
			continue
		}

		if b.consensus.WasSeen(block) {
			continue
		}

		if _, loaded := b.awaiting.LoadOrStore(blockID, true); loaded {
			continue
		}

		glog.V(1).Infof("%s received block %d:%s", b.Name(), block.BlockNumber(), blockID[:6])

		if !block.Valid() {
			glog.V(1).Infof("%s received invalid block %d:%s", b.Name(), block.BlockNumber(), blockID[:6])
			continue
		}

		validBlocks = append(validBlocks, block)
	}

	if len(validBlocks) > 0 {
		res := b.consensus.AddBlocks(validBlocks, local)
		if res.Error != nil {
			return res
		}
		if res.DisqualifiedBlocks != nil {
			for _, block := range res.DisqualifiedBlocks {
				glog.V(1).Infof("%s disqualified block %d:%s", b.Name(), block.BlockNumber(), block.Hash()[:6])
				b.awaiting.Delete(block.Hash())
			}
		}
		if res.AddedBlock != nil {
			glog.V(1).Infof("%s added and broadcasting block %d:%s", b.Name(), res.AddedBlock.BlockNumber(), res.AddedBlock.Hash()[:6])
			b.awaiting.Delete(res.AddedBlock.Hash())
		}
		return res
	}

	return nil
}

func (b *Blockchain) ReceiveTransaction(netMsg *spec.NetworkMessage) error {
	_, err := b.blockGenerator.ReceiveTransaction(netMsg)
	if err != nil {
		return err
	}
	return nil
}

func (b *Blockchain) GenerateGenesis() spec.Block {
	glog.Warningf("%s generating genesis block", b.Name())
	newBlock := b.blockGenerator.GenerateGenesisBlock()
	return newBlock
}

func (b *Blockchain) GenerateBlock(branch []spec.Block, rootID int) spec.Block {
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
	glog.Warningf("%s generating block %d for branch %s", b.Name(), head.BlockNumber()+1, branchLog)
	b.consensus.SetCompeted(head)
	newBlock := b.blockGenerator.GenerateBlock(branch)

	if newBlock == nil {
		return nil
	}
	return newBlock
}

func (b *Blockchain) confirmBlock(block spec.Block, local bool) {
	if b.confirmBlockNumber > 0 && block.BlockNumber() != b.confirmBlockNumber+1 {
		panic(fmt.Sprintf("blocked attempt to confirm out of order: block %d", block.BlockNumber()))
	}
	b.confirmBlockNumber = block.BlockNumber()
	b.blockGenerator.CommitBlock(block)
	if local {
		glog.Warningf("%s local block %d confirmed %s", b.Name(), block.BlockNumber(), color.HiGreenString(bold.Sprint(block.Hash()[:6])))
	} else {
		glog.Warningf("%s block %d confirmed %s", b.Name(), block.BlockNumber(), bold.Sprint(block.Hash()[:6]))
	}
}
