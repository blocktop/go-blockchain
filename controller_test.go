package blockchain_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/blckit/go-blockchain"
	"github.com/blckit/go-test-support/mock"
)

var _ = Describe("Controller", func() {

	Describe("#AddBlockchain", func() {

		It("adds blockchains and wires event handlers", func() {
			c := getInst()
			bc := c.Blockchains["mockchain"].(*mock.Blockchain)

			Expect(bc.OnBlockConfirmed).ToNot(BeNil())
			Expect(bc.OnLocalBlockConfirmed).ToNot(BeNil())
			Expect(bc.OnBlockGenerated).ToNot(BeNil())
		})
	})

	Describe("#AddTransactionHandler", func() {

		It("adds txn handler", func() {
			c := getInst()
			Expect(c.TransactionHandlers["mocktxn"]).ToNot(BeNil())
		})
	})

	Describe("#confirmBlock", func() {

		It("unlogs transactions and executes them", func() {
			c := getInst()
			block := mock.NewBlock("111", "222", uint64(1))
			txn := mock.NewTransaction("txn1")
			block.Transactions = append(block.Transactions, txn)
			bc := c.Blockchains["mockchain"].(*mock.Blockchain)
			mockGenerator := bc.GetBlockGenerator().(*mock.BlockGenerator)
			mockTxnHandler := c.TransactionHandlers["mocktxn"].(*mock.TransactionHandler)
			mockTxnHandler.HandleOK = true

			mockGenerator.LogTransaction(txn)
			Expect(mockGenerator.OutstandingTxns["txn1"]).ToNot(BeNil())

			bc.BlockConfirmed(block)

			time.Sleep(time.Millisecond)

			Expect(mockTxnHandler.ExecutedID).To(Equal("txn1"))
			Expect(mockGenerator.OutstandingTxns["txn1"]).To(BeNil())
		})
	})

	Describe("#confirmBlock", func() {

		It("logs and broadcasts block", func() {
			c := getInst()
			block := mock.NewBlock("111", "222", uint64(1))
			txn := mock.NewTransaction("txn1")
			block.Transactions = append(block.Transactions, txn)
			bc := c.Blockchains["mockchain"].(*mock.Blockchain)
			mockGenerator := bc.GetBlockGenerator().(*mock.BlockGenerator)
			mockTxnHandler := c.TransactionHandlers["mocktxn"].(*mock.TransactionHandler)
			mockTxnHandler.HandleOK = true
			
			mockGenerator.LogTransaction(txn)
			Expect(mockGenerator.OutstandingTxns["txn1"]).ToNot(BeNil())

			bc.BlockConfirmed(block)

			time.Sleep(time.Millisecond)

			Expect(mockTxnHandler.ExecutedID).To(Equal("txn1"))
			Expect(mockGenerator.OutstandingTxns["txn1"]).To(BeNil())
		})
	})
})

func getInst() *Controller {
	mockNetwork := &mock.Network{}
	c := NewController(mockNetwork, "peer123")
	bc := mock.NewBlockchain()
	c.AddBlockchain(bc)
	c.AddTransactionHandler(&mock.TransactionHandler{})

	return c
}
