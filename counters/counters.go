package counters

import (
	"bytes"
	"fmt"
	m20 "github.com/metrics20/go-metrics20"
	"github.com/vimeo/statsdaemon/common"
)

type Counters struct {
	prefixRates string
	values      map[string]float64
}

func New(prefixRates string) *Counters {
	return &Counters{
		prefixRates,
		make(map[string]float64),
	}
}

// Add updates the counters map, adding the metric key if needed
func (c *Counters) Add(metric *common.Metric) {
	_, ok := c.values[metric.Bucket]
	if !ok {
		c.values[metric.Bucket] = 0
	}
	c.values[metric.Bucket] += metric.Value * float64(1/metric.Sampling)
}

// processCounters computes the outbound metrics for counters and puts them in the buffer
func (c *Counters) Process(buffer *bytes.Buffer, now int64, interval int) int64 {
	var num int64
	for key, val := range c.values {
		val := val / float64(interval)
		fmt.Fprintf(buffer, "%s %f %d\n", m20.DeriveCount(key, c.prefixRates), val, now)
		num++
	}
	return num
}
