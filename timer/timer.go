package timer

import (
	"github.com/vimeo/statsdaemon/common"
)

type Float64Slice []float64

type Data struct {
	Points           Float64Slice
	Amount_submitted int64
}

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

// Add updates the timers map, adding the metric key if needed
func Add(timers map[string]Data, metric *common.Metric) {
	t, ok := timers[metric.Bucket]
	if !ok {
		var p Float64Slice
		t = Data{p, 0}
	}
	t.Points = append(t.Points, metric.Value)
	t.Amount_submitted += int64(1 / metric.Sampling)
	timers[metric.Bucket] = t
}
