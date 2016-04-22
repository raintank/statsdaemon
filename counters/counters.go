package counters

import (
	"bytes"
	"fmt"

	m20 "github.com/metrics20/go-metrics20"
	"github.com/vimeo/statsdaemon/common"
)

type Counters struct {
	prefixRates    string
	prefixCounters string
	sendRates      bool
	sendCounters   bool
	Values         map[string]float64
}

func New(sendRates bool, prefixRates string, sendCounters bool, prefixCounters string) *Counters {
	return &Counters{
		prefixRates,
		prefixCounters,
		sendRates,
		sendCounters,
		make(map[string]float64),
	}
}

// Add updates the counters map, adding the metric key if needed
func (c *Counters) Add(metric *common.Metric) {
	c.Values[metric.Bucket] += metric.Value * float64(1/metric.Sampling)
}

// processCounters computes the outbound metrics for counters and puts them in the buffer
func (c *Counters) Process(buffer *bytes.Buffer, now int64, interval int) int64 {
	var num int64
	for key, val := range c.Values {
		if c.sendCounters {
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Count(key, c.prefixCounters), val, now)
		}

		if c.sendRates {
			val := val / float64(interval)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.DeriveCount(key, c.prefixRates), val, now)
		}

		num++
	}
	return num
}
