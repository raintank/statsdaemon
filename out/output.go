package out

import (
	"github.com/raintank/statsdaemon/common"
	"github.com/tv42/topic"
)

type Output struct {
	Metrics       chan []*common.Metric
	MetricAmounts chan []*common.Metric
	Valid_lines   *topic.Topic
	Invalid_lines *topic.Topic
}

func NullOutput() *Output {
	output := Output{
		Metrics:       make(chan []*common.Metric),
		MetricAmounts: make(chan []*common.Metric),
		Valid_lines:   topic.New(),
		Invalid_lines: topic.New(),
	}
	go func() {
		for {
			<-output.Metrics
		}
	}()
	go func() {
		for {
			<-output.MetricAmounts
		}
	}()
	return &output
}
