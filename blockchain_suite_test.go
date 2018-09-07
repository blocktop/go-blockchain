package blockchain

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoBlockchain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Blockchain Suite")
}
