package main

import (
	"github.com/vimeo/statsdaemon/common"
	"math/rand"
	"strconv"
)

func getDifferentCounters(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "count" + strconv.Itoa(i)
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "c", sampling, 0}
	}
	return metrics
}

func getSameCounters(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "count"
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "c", sampling, 0}
	}
	return metrics
}

func getDifferentGauges(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "gauge" + strconv.Itoa(i)
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "g", sampling, 0}
	}
	return metrics
}

func getSameGauges(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "gauge"
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "g", sampling, 0}
	}
	return metrics
}

func getDifferentTimers(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "timer" + strconv.Itoa(i)
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "ms", sampling, 0}
	}
	return metrics
}

func getSameTimers(amount int) []common.Metric {
	r := rand.New(rand.NewSource(438))
	metrics := make([]common.Metric, amount)
	for i := 0; i < amount; i++ {
		bucket := "timer"
		val := r.Float64()
		sampling := r.Float32()
		metrics[i] = common.Metric{bucket, val, "ms", sampling, 0}
	}
	return metrics
}
