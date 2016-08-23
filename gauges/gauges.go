package gauges

import (
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/statsdaemon/common"
)

type Gauges struct {
	prefix string
	Values map[string]float64
}

func New(prefix string) *Gauges {
	return &Gauges{
		prefix,
		make(map[string]float64),
	}
}

// Add updates the gauges with the latest value for given key
func (g *Gauges) Add(metric *common.Metric) {
	g.Values[metric.Bucket] = metric.Value
}

// Process puts gauges in the outbound buffer
func (g *Gauges) Process(buf []byte, now int64, interval int) ([]byte, int64) {
	var num int64
	for key, val := range g.Values {
		buf = common.WriteFloat64(buf, []byte(m20.Gauge(key, g.prefix)), val, now)
		num++
	}
	return buf, num
}
