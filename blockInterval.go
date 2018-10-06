package blockchain

import (
	"time"

	"github.com/fatih/color"
	"github.com/golang/glog"
	"github.com/spf13/viper"

	"github.com/mxmCherry/movavg"
)

type blockInterval struct {
	interval       int
	consensusDepth int
	evalHeadTime   *movavg.SMA
	genBlockTime   *movavg.SMA
	addBlockTime   *movavg.SMA
	confBlockTime  *movavg.SMA
}

func newBlockInterval() *blockInterval {
	i := &blockInterval{}
	i.interval = int(viper.GetDuration("blockchain.blockinterval") / time.Microsecond)
	i.consensusDepth = viper.GetInt("blockchain.consensus.depth")
	i.evalHeadTime = movavg.NewSMA(i.interval * i.consensusDepth)
	i.genBlockTime = movavg.NewSMA(i.interval * i.consensusDepth)
	i.addBlockTime = movavg.NewSMA(i.interval * i.consensusDepth)
	i.confBlockTime = movavg.NewSMA(i.interval * i.consensusDepth)

	return i
}

func (i *blockInterval) addEvalHeadTime(nanos int64) {
	i.evalHeadTime.Add(float64(nanos / int64(time.Millisecond)))
}

func (i *blockInterval) addGenBlockTime(nanos int64) {
	i.genBlockTime.Add(float64(nanos / int64(time.Millisecond)))
}

func (i *blockInterval) addAddBlockTime(nanos int64) {
	i.confBlockTime.Add(float64(nanos / int64(time.Millisecond)))
}

func (i *blockInterval) addConfBlockTime(nanos int64) {
	i.confBlockTime.Add(float64(nanos / int64(time.Millisecond)))
}

func (i *blockInterval) getNetworkTime() time.Duration {
	evalAvg := i.evalHeadTime.Avg()
	addAvg := i.addBlockTime.Avg()
	confAvg := i.confBlockTime.Avg()

	netTime := float64(i.interval) - evalAvg - addAvg - confAvg

	glog.V(2).Infof("Block interval, head evaluation time: %fµs", evalAvg)
	glog.V(2).Infof("Block interval, block gen/recv time:  %fµs", netTime)
	glog.V(2).Infof("Block interval, block add time:       %fµs", addAvg)
	glog.V(2).Infof("Block interval, block confirm time:   %fµs", confAvg)

	if netTime < 0 {
		glog.Errorln(color.HiRedString("Block interval too fast by %fµs", netTime))
	}

	return time.Duration(int64(netTime) * int64(time.Microsecond))
}
