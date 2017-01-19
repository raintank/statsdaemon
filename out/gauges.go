package out

import (
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/statsdaemon/common"
)

type Gauges struct {
	Values map[string]float64
}

func NewGauges() *Gauges {
	return &Gauges{
		make(map[string]float64),
	}
}

// Add updates the gauges with the latest value for given key
func (g *Gauges) Add(metric *common.Metric) {
	g.Values[metric.Bucket] = metric.Value
}

// Process puts gauges in the outbound buffer
func (g *Gauges) Process(buf []byte, now int64, interval int, f Formatter) ([]byte, int64) {
	var num int64
	for key, val := range g.Values {
		key = m20.Gauge(key, f.Prefix_gauges, f.Prefix_m20_gauges, f.Prefix_m20ne_gauges)
		buf = WriteFloat64(buf, []byte(key), val, now)
		num++
	}
	return buf, num
}
