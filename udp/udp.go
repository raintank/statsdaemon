package udp

import (
	"bytes"
	"fmt"
	"github.com/tv42/topic"
	"github.com/vimeo/statsdaemon/common"
	"log"
	"net"
	"strconv"
)

const (
	MaxUdpPacketSize = 65535
)

// ParseLine turns a line into a *Metric (or not) and returns whether the line was valid.
// note that *Metric can be nil when the line was valid (if the line was empty)
func ParseLine(line []byte) (metric *common.Metric, valid bool) {
	if len(line) == 0 {
		return nil, true
	}
	parts := bytes.SplitN(line, []byte(":"), 2)
	if len(parts) != 2 {
		return nil, false
	}
	if bytes.Contains(parts[1], []byte(":")) {
		return nil, false
	}
	bucket := parts[0]
	parts = bytes.SplitN(parts[1], []byte("|"), 3)
	if len(parts) < 2 {
		return nil, false
	}
	modifier := string(parts[1])
	if modifier != "g" && modifier != "c" && modifier != "ms" {
		return nil, false
	}
	sampleRate := float64(1)
	if len(parts) == 3 {
		if parts[2][0] != byte('@') {
			return nil, false
		}
		var err error
		sampleRate, err = strconv.ParseFloat(string(parts[2])[1:], 32)
		if err != nil {
			return nil, false
		}
	}
	value, err := strconv.ParseFloat(string(parts[0]), 64)
	if err != nil {
		log.Printf("ERROR: failed to parse value in line '%s' - %s\n", line, err)
		return nil, false
	}
	metric = &common.Metric{
		Bucket:   string(bucket),
		Value:    value,
		Modifier: modifier,
		Sampling: float32(sampleRate),
	}
	return metric, true
}

// ParseMessage turns byte data into a slice of metric pointers
// note that it creates "invalid line" metrics itself, upon invalid lines,
// which will get passed on and aggregated along with the other metrics
func ParseMessage(data []byte, prefix_internal string, invalid_lines *topic.Topic) []*common.Metric {
	var output []*common.Metric
	for _, line := range bytes.Split(data, []byte("\n")) {
		metric, valid := ParseLine(line)
		if !valid {
            if invalid_lines != nil {
                invalid_lines.Broadcast <- line
            }
			metric = &common.Metric{
				fmt.Sprintf("%starget_type=count.type=invalid_line.unit=Err", prefix_internal),
				float64(1),
				"c",
				float32(1)}
		}
		if metric != nil {
			output = append(output, metric)
		}
	}
	return output
}

// Listener receives packets from the udp buffer, parses them and feeds both the Metrics channel
// as well as the metricAmountCollector channel
func Listener(listen_addr, prefix_internal string, Metrics chan *common.Metric, metricAmountCollector chan common.MetricAmount, invalid_lines *topic.Topic) {
	address, err := net.ResolveUDPAddr("udp", listen_addr)
	if err != nil {
		log.Fatalf("ERROR: Cannot resolve '%s' - %s", listen_addr, err)
	}

	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()
	log.Printf("listening on %s", address)

	message := make([]byte, MaxUdpPacketSize)
	for {
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		for _, p := range ParseMessage(message[:n], prefix_internal, invalid_lines) {
			Metrics <- p
			metricAmountCollector <- common.MetricAmount{p.Bucket, p.Sampling}
		}
	}
}
