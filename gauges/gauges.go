package gauges

import (
	"bytes"
	"fmt"
	m20 "github.com/metrics20/go-metrics20"
	"github.com/vimeo/statsdaemon/common"
)

type Gauges struct {
	prefix string
	values map[string]float64
}

func New(prefix string) *Gauges {
	return &Gauges{
		prefix,
		make(map[string]float64),
	}
}

// Add updates the gauges with the latest value for given key
func (g *Gauges) Add(metric *common.Metric) {
	g.values[metric.Bucket] = metric.Value
}

// Process puts gauges in the outbound buffer
func (g *Gauges) Process(buffer *bytes.Buffer, now int64, interval int) int64 {
	var num int64
	for key, val := range g.values {
		fmt.Fprintf(buffer, "%s %f %d\n", m20.Gauge(key, g.prefix), val, now)
		num++
	}
	return num
}
