package indicator

import (
	"fmt"

	"kpokjn/internal/logx"
)

type Result struct {
	Signal    string
	FastSMA   float64
	SlowSMA   float64
	Price     float64
}

func (r Result) String() string {
	return fmt.Sprintf("Signal=%s | Price=%.2f | FastSMA=%.2f | SlowSMA=%.2f",
		r.Signal, r.Price, r.FastSMA, r.SlowSMA)
}

func SmaCrossover(closes []float64, fastPeriod, slowPeriod int) Result {
	if len(closes) < slowPeriod+1 {
		logx.Warnf("Not enough data for SMA crossover: got %d, need %d", len(closes), slowPeriod+1)
		return Result{Signal: "HOLD"}
	}

	fast := sma(closes, len(closes)-fastPeriod, fastPeriod)
	slow := sma(closes, len(closes)-slowPeriod, slowPeriod)

	prevFast := sma(closes, len(closes)-1-fastPeriod, fastPeriod)
	prevSlow := sma(closes, len(closes)-1-slowPeriod, slowPeriod)

	price := closes[len(closes)-1]

	signal := "HOLD"
	if prevFast <= prevSlow && fast > slow {
		signal = "BUY"
	} else if prevFast >= prevSlow && fast < slow {
		signal = "SELL"
	}

	logx.Debugf("SMA crossover: fast=%.2f slow=%.2f prevFast=%.2f prevSlow=%.2f => %s",
		fast, slow, prevFast, prevSlow, signal)

	return Result{
		Signal:  signal,
		FastSMA: fast,
		SlowSMA: slow,
		Price:   price,
	}
}

func sma(data []float64, end, period int) float64 {
	if end < period-1 {
		return 0
	}
	start := end - period
	sum := 0.0
	for i := start; i < end; i++ {
		sum += data[i]
	}
	return sum / float64(period)
}
