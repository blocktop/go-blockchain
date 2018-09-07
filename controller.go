package blockchain

import (
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"

	spec "github.com/blckit/go-spec"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

type Controller struct {
	Blockchains         map[string]spec.Blockchain
	TransactionHandlers map[string]spec.TransactionHandler
	Network             spec.Network
	PeerID              string
}

const MessageHeaderVersion = "v1"

func NewController(network spec.Network, peerID string) *Controller {
	c := &Controller{Network: network, PeerID: peerID}

	c.Blockchains = make(map[string]spec.Blockchain)
	c.TransactionHandlers = make(map[string]spec.TransactionHandler)
	c.Network.OnMessageReceived(func(message proto.Message, p *spec.MessageProtocol) {
		c.reseceiveMessage(message, p)
	})

	return c
}

func (c *Controller) AddBlockchain(bc spec.Blockchain) {
	bcType := bc.GetType()
	if c.Blockchains[bcType] != nil {
		panic("blockchain already added")
	}
	c.Blockchains[bcType] = bc
	bc.OnBlockConfirmed(func(block spec.Block) {
		glog.Infof("Peer %s: %s confirmed block %d: %s", c.PeerID[:6], bc.GetType(), block.GetBlockNumber(), block.GetID()[:6])
		c.confirmBlock(bcType, block)
	})
	bc.OnLocalBlockConfirmed(func(block spec.Block) {
		glog.Infof("Peer %s: %s confirmed local block %d: %s", c.PeerID[:6], bc.GetType(), block.GetBlockNumber(), block.GetID()[:6])
		c.confirmBlock(bcType, block)
		c.confirmLocalBlock(bcType, block)
	})
	bc.OnBlockGenerated(func(block spec.Block) {
		glog.Infof("Peer %s: %s generated block %d: %s", c.PeerID[:6], bc.GetType(), block.GetBlockNumber(), block.GetID()[:6])
		c.broadcastBlock(bcType, block)
	})
}

func (c *Controller) AddTransactionHandler(h spec.TransactionHandler) {
	txnType := h.GetType()
	if c.TransactionHandlers[txnType] != nil {
		panic("transaction handler already added")
	}
	c.TransactionHandlers[txnType] = h
}

func (c *Controller) StartBlockchains() {
	for name := range c.Blockchains {
		c.StartBlockchain(name)
	}
}

func (c *Controller) StartBlockchain(name string) {
	c.Blockchains[name].Start()
}

func (c *Controller) StopBlockchains() {
	for name := range c.Blockchains {
		c.StopBlockchain(name)
	}
}

func (c *Controller) StopBlockchain(name string) {
	c.Blockchains[name].Stop()
}

func (c *Controller) confirmBlock(bcType string, block spec.Block) {
	go c.unlogTransactions(bcType, block)
	go c.executeTransactions(bcType, block)
}

func (c *Controller) unlogTransactions(bcType string, block spec.Block) {
	bc := c.Blockchains[bcType]
	if bc == nil {
		// TODO: log something, fail?
		return
	}
	bc.GetBlockGenerator().UnlogTransactions(block.GetTransactions())
}

func (c *Controller) executeTransactions(bcType string, block spec.Block) {
	for _, t := range block.GetTransactions() {
		txnType := t.GetType()
		handler := c.TransactionHandlers[txnType]
		if handler == nil {
			// TODO: log something here
			// if we can't confirm txn then our data will be corrupt
			// or no one else will be able to either
			// or could be security issue
		} else {
			handler.Execute(t)
		}
	}
}

func (c *Controller) confirmLocalBlock(bcType string, block spec.Block) {
	// hooray!
	// TODO: Log our success and our reward
	glog.Warningf("Peer %s: %s local block confirmed, reward = %d", c.PeerID[:6], bcType, 1000000000	)
}

func (c *Controller) broadcastBlock(bcType string, block spec.Block) {
	p := &spec.MessageProtocol{}
	p.SetBlockchainType(bcType)
	p.SetResourceType(spec.ResourceTypeBlock)
	p.SetComponentType(block.GetType())
	p.SetVersion(block.GetVersion())

	id := block.GetID()
	uri := spec.NewURIFromProtocol(p, id)

	header := &MessageHeader{
		Version:   MessageHeaderVersion,
		ID:        uuid.New().String(),
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		From:      c.PeerID,
		URI:       uri.String()}

	message := block.Marshal()
	a, err := ptypes.MarshalAny(message)
	if err != nil {
		//TODO
		return
	}
	header.Message = a

	c.Network.Broadcast(header, p)
}

func (c *Controller) reseceiveMessage(message proto.Message, p *spec.MessageProtocol) {
	bcType := p.GetBlockchainType()
	if !p.IsValid() {
		// something not right, what to do?
		return
	}
	bc := c.Blockchains[bcType]
	if bc == nil {
		// TODO: log, fail, what? why is someone sending us this?
		return
	}

	header := message.(*MessageHeader)
	if !c.verifyHeader(header) {
		//TODO
		return
	}

	switch p.GetResourceType() {
	case "transaction":
		go c.receiveTransaction(bc, header.GetMessage(), p)

	case "block":
		go c.receiveBlock(bc, header.GetMessage())
	}
}

func (c *Controller) verifyHeader(header *MessageHeader) bool {
	return true
}

func (c *Controller) receiveTransaction(bc spec.Blockchain, message proto.Message, p *spec.MessageProtocol) {
	txnType := p.GetComponentType()
	h := c.TransactionHandlers[txnType]
	if h == nil {
		//TODO
		return
	}
	txn := h.Unmarshal(message)
	bc.GetBlockGenerator().LogTransaction(txn)
}

func (c *Controller) receiveBlock(bc spec.Blockchain, message proto.Message) {
	block := bc.GetBlockGenerator().Unmarshal(message, c.TransactionHandlers)
	glog.Infof("Peer %s: %s received block %s", c.PeerID[:6], bc.GetType(), block.GetID()[:6])
	bc.ReceiveBlock(block)
	//TODO: re-broadcast better block if we have one
}
