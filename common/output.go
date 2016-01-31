package common

import (
	"github.com/tv42/topic"
)

type Output struct {
	Metrics       chan []*Metric
	MetricAmounts chan []*Metric
	Valid_lines   *topic.Topic
	Invalid_lines *topic.Topic
}

func NullOutput() *Output {
	output := Output{
		Metrics:       make(chan []*Metric),
		MetricAmounts: make(chan []*Metric),
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
