package out

import (
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/statsdaemon/common"
)

type Counters struct {
	flushRates  bool
	flushCounts bool
	Values      map[string]float64
}

func NewCounters(flushRates, flushCounts bool) *Counters {
	return &Counters{
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
func (c *Counters) Process(buf []byte, now int64, interval int, f Formatter) ([]byte, int64) {
	for key, val := range c.Values {
		if c.flushCounts {
			key := m20.Count(key, f.Prefix_counters, f.Prefix_m20_counters, f.Prefix_m20ne_counters, f.Legacy_namespace)
			buf = WriteFloat64(buf, []byte(key), val, now)
		}

		if c.flushRates {
			key := m20.DeriveCount(key, f.Prefix_rates, f.Prefix_m20_rates, f.Prefix_m20ne_rates, f.Legacy_namespace)
			buf = WriteFloat64(buf, []byte(key), val/float64(interval), now)
		}
	}
	return buf, int64(len(c.Values))
}
