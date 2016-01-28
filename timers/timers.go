package timers

import (
	"bytes"
	"fmt"
	m20 "github.com/metrics20/go-metrics20"
	"github.com/vimeo/statsdaemon/common"
	"math"
	"sort"
)

type Float64Slice []float64

type Timers struct {
	prefix string
	pctls  Percentiles
	Values map[string]Data
}

func New(prefix string, pctls Percentiles) *Timers {
	return &Timers{
		prefix,
		pctls,
		make(map[string]Data),
	}
}

type Data struct {
	Points           Float64Slice
	Amount_submitted int64
}

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

func (t *Timers) String() string {
	return fmt.Sprintf("<*Timers %p prefix '%s', percentiles '%s', %d values>", t, t.prefix, t.pctls, len(t.Values))
}

// Add updates the timers map, adding the metric key if needed
func (timers *Timers) Add(metric *common.Metric) {
	t, ok := timers.Values[metric.Bucket]
	if !ok {
		var p Float64Slice
		t = Data{p, 0}
	}
	t.Points = append(t.Points, metric.Value)
	t.Amount_submitted += int64(1 / metric.Sampling)
	timers.Values[metric.Bucket] = t
}

// Process computes the outbound metrics for timers and puts them in the buffer
func (timers *Timers) Process(buffer *bytes.Buffer, now int64, interval int) int64 {
	// these are the metrics that get exposed:
	// count estimate of original amount of metrics sent, by dividing received by samplerate
	// count_ps  same but per second
	// lower
	// mean  // arithmetic mean
	// mean_<pct> // arithmetic mean of values below <pct> percentile
	// median
	// std  standard deviation
	// sum
	// sum_90
	// upper
	// upper_90 / lower_90

	var num int64
	for u, t := range timers.Values {
		if len(t.Points) > 0 {
			seen := len(t.Points)
			count := t.Amount_submitted
			count_ps := float64(count) / float64(interval)
			num++

			sort.Sort(t.Points)
			min := t.Points[0]
			max := t.Points[seen-1]

			sum := float64(0)
			for _, value := range t.Points {
				sum += value
			}
			mean := float64(sum) / float64(seen)
			sumOfDiffs := float64(0)
			for _, value := range t.Points {
				sumOfDiffs += math.Pow((float64(value) - mean), 2)
			}
			stddev := math.Sqrt(sumOfDiffs / float64(seen))
			mid := seen / 2
			var median float64
			if seen%2 == 1 {
				median = t.Points[mid]
			} else {
				median = (t.Points[mid-1] + t.Points[mid]) / 2
			}
			var cumulativeValues Float64Slice
			cumulativeValues = make(Float64Slice, seen, seen)
			cumulativeValues[0] = t.Points[0]
			for i := 1; i < seen; i++ {
				cumulativeValues[i] = t.Points[i] + cumulativeValues[i-1]
			}

			maxAtThreshold := max
			sum_pct := sum
			mean_pct := mean

			for _, pct := range timers.pctls {

				if seen > 1 {
					var abs float64
					if pct.float >= 0 {
						abs = pct.float
					} else {
						abs = 100 + pct.float
					}
					// poor man's math.Round(x):
					// math.Floor(x + 0.5)
					indexOfPerc := int(math.Floor(((abs / 100.0) * float64(seen)) + 0.5))
					if pct.float >= 0 {
						sum_pct = cumulativeValues[indexOfPerc-1]
						maxAtThreshold = t.Points[indexOfPerc-1]
					} else {
						maxAtThreshold = t.Points[indexOfPerc]
						sum_pct = cumulativeValues[seen-1] - cumulativeValues[seen-indexOfPerc-1]
					}
					mean_pct = float64(sum_pct) / float64(indexOfPerc)
				}

				var pctstr string
				var fn func(metric_in, prefix, percentile, timespec string) string
				if pct.float >= 0 {
					pctstr = pct.str
					fn = m20.Max
				} else {
					pctstr = pct.str[1:]
					fn = m20.Min
				}
				fmt.Fprintf(buffer, "%s %f %d\n", fn(u, timers.prefix, pctstr, ""), maxAtThreshold, now)
				fmt.Fprintf(buffer, "%s %f %d\n", m20.Mean(u, timers.prefix, pctstr, ""), mean_pct, now)
				fmt.Fprintf(buffer, "%s %f %d\n", m20.Sum(u, timers.prefix, pctstr, ""), sum_pct, now)
			}

			fmt.Fprintf(buffer, "%s %f %d\n", m20.Mean(u, timers.prefix, "", ""), mean, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Median(u, timers.prefix, "", ""), median, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Std(u, timers.prefix, "", ""), stddev, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Sum(u, timers.prefix, "", ""), sum, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Max(u, timers.prefix, "", ""), max, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.Min(u, timers.prefix, "", ""), min, now)
			fmt.Fprintf(buffer, "%s %d %d\n", m20.CountPckt(u, timers.prefix), count, now)
			fmt.Fprintf(buffer, "%s %f %d\n", m20.RatePckt(u, timers.prefix), count_ps, now)
		}
	}
	return num
}
