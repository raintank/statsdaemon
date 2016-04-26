package counters

import (
	"bytes"
	"fmt"

	m20 "github.com/metrics20/go-metrics20"
	"github.com/vimeo/statsdaemon/common"
)

type Counters struct {
	prefixRates     string
	prefixCounters  string
	legacyNamespace bool
	Values          map[string]float64
}

func New(prefixRates string, prefixCounters string, legacyNamespace bool) *Counters {
	return &Counters{
		prefixRates,
		prefixCounters,
		legacyNamespace,
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
		if c.legacyNamespace {
			fmt.Fprintf(buffer, "%s %f %d\n", m20.DeriveCount(key, c.prefixCounters), val, now)

			val := val / float64(interval)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.DeriveCount(key, c.prefixRates), val, now)

			num += 2
		} else {
			// legacyNamesace = false
			// adds `.count`  and `.rate` suffix
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Count(key, c.prefixCounters), val, now)

			val := val / float64(interval)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.DeriveCount(key, c.prefixRates)+".rate", val, now)
			num += 2
		}
	}
	return num
}
