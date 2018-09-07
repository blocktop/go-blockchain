package blockchain

import (
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	spec "github.com/blckit/go-spec"
	"github.com/blckit/go-test-support/mock"
	//depth "github.com/blckit/go-consensus-depth"
	//cons "github.com/blckit/go-consensus"
)

var _ = Describe("Blockchain", func() {

	Describe("#New", func() {

		It("creates instance and sets up commit handler", func() {
			b := GetInst()
			Expect(b.BlockInterval).To(Equal(DefaultBlockInterval))
			Expect(b.localBlocks).ToNot(BeNil())
			Expect(b.Conesensus.OnBlockConfirmed).ToNot(BeNil())
		})
	})

	Describe("#ReceiveBlock", func() {

		It("rejects invalid block", func() {
			b := GetInst()
			block := mock.NewBlock("111", "000", 1)
			block.IsValid = false
			b.ReceiveBlock(block)

			// wait for goroutine
			time.Sleep(100 * time.Microsecond)
			mockConsensus := b.Conesensus.(*mock.Consensus)
			Expect(mockConsensus.AddBlockCalled).To(BeFalse())

		})

		It("rejects already seen block", func() {
			b := GetInst()
			block := mock.NewBlock("111", "000", 1)
			mockConsensus := b.Conesensus.(*mock.Consensus)
			mockConsensus.BlockWasSeen = true
			b.ReceiveBlock(block)

			// wait for goroutine
			time.Sleep(100 * time.Microsecond)
			Expect(mockConsensus.AddBlockCalled).To(BeFalse())
		})

		It("rejects awaiting validation", func() {
			b := GetInst()
			block := mock.NewBlock("111", "000", 1)
			b.awaiting = append(b.awaiting, "111")
			b.ReceiveBlock(block)

			// wait for goroutine
			time.Sleep(100 * time.Microsecond)
			mockConsensus := b.Conesensus.(*mock.Consensus)
			Expect(mockConsensus.AddBlockCalled).To(BeFalse())
		})

		It("adds block", func() {
			b := GetInst()
			block := mock.NewBlock("111", "000", 1)
			block.IsValid = true
			b.ReceiveBlock(block)

			// wait for goroutine
			time.Sleep(100 * time.Microsecond)
			mockConsensus := b.Conesensus.(*mock.Consensus)
			Expect(mockConsensus.AddBlockCalled).To(BeTrue())
		})
	})

	Describe("#mainLoop", func() {

		It("continues if no branch available", func() {
			b := GetInst()
			b.Start()

			time.Sleep(3 * time.Millisecond)

			mockConsensus := b.Conesensus.(*mock.Consensus)
			Expect(mockConsensus.AddBlockCalled).To(BeFalse())

			b.Stop()
		})

		It("continues if branch head not old enough", func() {
			b := GetInst()
			b1 := mock.NewBlock("111", "000", 1)
			b2 := mock.NewBlock("222", "111", 2)
			mockConsensus := b.Conesensus.(*mock.Consensus)
			mockConsensus.BestBranch = []spec.Block{b2, b1}

			b.Start()

			time.Sleep(3 * time.Millisecond)

			Expect(mockConsensus.AddBlockCalled).To(BeFalse())

			b.Stop()
		})

		It("does not add new block if it is not valid", func() {
			b := GetInst()
			b1 := mock.NewBlock("111", "000", 1)
			b2 := mock.NewBlock("222", "111", 2)
			b2.Timestamp = time.Now().UnixNano()/int64(time.Millisecond) - DefaultBlockInterval
			mockConsensus := b.Conesensus.(*mock.Consensus)
			mockConsensus.BestBranch = []spec.Block{b2, b1}
			mockGenerator := b.GetBlockGenerator().(*mock.BlockGenerator)
			mockGenerator.NewBlockValid = false

			b.Start()

			time.Sleep(3 * time.Millisecond)

			Expect(mockConsensus.AddBlockCalled).To(BeFalse())

			b.Stop()
		})

		It("adds new block", func(done Done) {
			b := GetInst()
			b1 := mock.NewBlock("111", "000", 1)
			b2 := mock.NewBlock("222", "111", 2)
			b2.Timestamp = time.Now().UnixNano()/int64(time.Millisecond) - DefaultBlockInterval
			mockConsensus := b.Conesensus.(*mock.Consensus)
			mockConsensus.BestBranch = []spec.Block{b2, b1}
			mockGenerator := b.GetBlockGenerator().(*mock.BlockGenerator)
			mockGenerator.NewBlockValid = true

			once := sync.Once{}
			b.OnBlockGenerated(func(block spec.Block) {
				Expect(mockConsensus.AddBlockCalled).To(BeTrue())
				Expect(block.GetParentID()).To(Equal("222"))
				Expect(block.GetID()[:4]).To(Equal("222-"))

				b.Stop()
				once.Do(func() {
					close(done)
				})
			})

			b.Start()
		})
	})

	Describe("#OnBlockConfirmed", func() {

		It("calls OnBlockConfirmed when a block is confirmed", func(done Done) {
			b := GetInst()
			b1 := mock.NewBlock("111", "000", 1)
			b2 := mock.NewBlock("222", "111", 2)
			b1.Timestamp = time.Now().UnixNano()/int64(time.Millisecond) - DefaultBlockInterval
			b2.Timestamp = time.Now().UnixNano()/int64(time.Millisecond) - DefaultBlockInterval
			mockConsensus := b.Conesensus.(*mock.Consensus)
			mockConsensus.BestBranch = []spec.Block{b1}

			b.OnBlockGenerated(func(block spec.Block) {
				mockConsensus.BestBranch = nil
				b.Stop()
				Expect(b.localBlocks[0].GetParentID()).To(Equal("111"))
				mockConsensus.BestBranch = []spec.Block{b2, b1}
				mockConsensus.ConfirmBlock = true
				b.Start()
			})
			once := sync.Once{}
			mockConsensus.OnBlockConfirmed(func(block spec.Block) {
				Expect(block.GetParentID()).To(Equal("222"))
				once.Do(func() {
					close(done)
				})				
			})

			b.Start()
		})
	})
})

func BenchmarkReceiveBlock(b *testing.B) {
	blockIDi := 111
	parentID := "000"
	blockNumber := uint64(1)
	bc := GetInst()
	for n := 0; n < b.N; n++ {
		blockID := strconv.FormatInt(int64(blockIDi), 10)
		block := mock.NewBlock(blockID, parentID, blockNumber)
		bc.ReceiveBlock(block)
		blockNumber++
		blockIDi++
		parentID = blockID
	}
}

func GetInst() *Blockchain {
	blockGen := mock.NewBlockGenerator()
	//consSpec := depth.DepthConsensus{}
	//compSpec := depth.DepthCompetiton{}
	consensus := mock.NewConsensus()
	return NewBlockchain("testchain", blockGen, consensus)
}
