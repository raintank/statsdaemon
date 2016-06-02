package counters

import (
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/vimeo/statsdaemon/common"
)

type Counters struct {
	prefixRates     string
	prefixCounters  string
	legacyNamespace bool
	flushRates      bool
	flushCounts     bool
	Values          map[string]float64
}

func New(prefixRates string, prefixCounters string, legacyNamespace, flushRates, flushCounts bool) *Counters {
	return &Counters{
		prefixRates,
		prefixCounters,
		legacyNamespace,
		flushRates,
		flushCounts,
		make(map[string]float64),
	}
}

// Add updates the counters map, adding the metric key if needed
func (c *Counters) Add(metric *common.Metric) {
	c.Values[metric.Bucket] += metric.Value * float64(1/metric.Sampling)
}

// processCounters computes the outbound metrics for counters and puts them in the buffer
func (c *Counters) Process(buf []byte, now int64, interval int) ([]byte, int64) {
	for key, val := range c.Values {
		if c.flushCounts {
			buf = common.WriteFloat64(buf, []byte(m20.Count(key, c.prefixCounters, c.legacyNamespace)), val, now)
		}

		if c.flushRates {
			buf = common.WriteFloat64(buf, []byte(m20.DeriveCount(key, c.prefixRates, c.legacyNamespace)), val/float64(interval), now)
		}
	}
	return buf, int64(len(c.Values))
}
