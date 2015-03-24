package udp

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vimeo/statsdaemon/common"
	"log"
	"net"
	"strconv"
)

const (
	MaxUdpPacketSize = 65535
)

// ParseLine turns a line into a *Metric (or not) and returns an error if the line was invalid.
// note that *Metric can be nil when the line was valid (if the line was empty)
// input format: key:value|modifier[|@samplerate]
func ParseLine(line []byte) (metric *common.Metric, err error) {
	if len(line) == 0 {
		return nil, nil
	}
	parts := bytes.SplitN(bytes.TrimSpace(line), []byte(":"), 2)
	if len(parts) != 2 {
		return nil, errors.New("bad amount of colons")
	}
	if bytes.Contains(parts[1], []byte(":")) {
		return nil, errors.New("bad amount of colons")
	}
	bucket := parts[0]
	if len(bucket) == 0 {
		return nil, errors.New("key zero len")
	}
	parts = bytes.SplitN(parts[1], []byte("|"), 3)
	if len(parts) < 2 {
		return nil, errors.New("bad amount of pipes")
	}
	modifier := string(parts[1])
	if modifier != "g" && modifier != "c" && modifier != "ms" {
		return nil, errors.New("unsupported metric type")
	}
	sampleRate := float64(1)
	if len(parts) == 3 {
		if parts[2][0] != byte('@') {
			return nil, errors.New("invalid sampling")
		}
		var err error
		sampleRate, err = strconv.ParseFloat(string(parts[2])[1:], 32)
		if err != nil {
			return nil, err
		}
	}
	value, err := strconv.ParseFloat(string(parts[0]), 64)
	if err != nil {
		return nil, err
	}
	metric = &common.Metric{
		Bucket:   string(bucket),
		Value:    value,
		Modifier: modifier,
		Sampling: float32(sampleRate),
	}
	return metric, nil
}

// ParseArchiveLine turns an "archive line" into an *Metric (or not) and returns an error if the line was invalid.
// note that *Metric can be nil when the line was valid (if the line was empty)
// input format: key value ts modifier [samplerate]
func ParseArchiveLine(line []byte) (metric *common.Metric, err error) {
	if len(line) == 0 {
		return nil, nil
	}
	parts := bytes.SplitN(bytes.TrimSpace(line), []byte(" "), 6)
	if len(parts) != 4 && len(parts) != 5 {
		return nil, errors.New("incorrect amount of fields")
	}
	bucket := parts[0]
	value, err := strconv.ParseFloat(string(parts[1]), 64)
	if err != nil {
		return nil, errors.New("couldn't parseFloat value")
	}
	ts, err := strconv.ParseUint(string(parts[2]), 32, 32)
	modifier := string(parts[3])
	if modifier != "g" && modifier != "c" && modifier != "ms" {
		return nil, errors.New("unsupported metric type")
	}
	sampleRate := float64(1)
	if len(parts) == 5 {
		var err error
		sampleRate, err = strconv.ParseFloat(string(parts[4]), 32)
		if err != nil {
			return nil, errors.New("couldn't parseFloat sampling")
		}
	}
	metric = &common.Metric{
		Bucket:   string(bucket),
		Value:    value,
		Modifier: modifier,
		Sampling: float32(sampleRate),
		Time:     uint32(ts),
	}
	return metric, nil
}

// ParseMessage turns byte data into a slice of metric pointers
// note that it creates "invalid line" metrics itself, upon invalid lines,
// which will get passed on and aggregated along with the other metrics
func ParseMessage(data []byte, prefix_internal string, output *common.Output, parse parseLineFunc) (metrics []*common.Metric) {
	for _, line := range bytes.Split(data, []byte("\n")) {
		metric, err := parse(line)
		if err != nil {
			// data will be repurposed by the udpListener
			report_line := make([]byte, len(line), len(line))
			copy(report_line, line)
			output.Invalid_lines.Broadcast <- report_line
			metric = &common.Metric{
				fmt.Sprintf("%starget_type_is_count.type_is_invalid_line.unit_is_Err", prefix_internal),
				float64(1),
				"c",
				float32(1),
				0,
			}
		} else {
			// data will be repurposed by the udpListener
			report_line := make([]byte, len(line), len(line))
			copy(report_line, line)
			output.Valid_lines.Broadcast <- report_line
		}
		if metric != nil {
			metrics = append(metrics, metric)
		}
	}
	return metrics
}

type parseLineFunc func(line []byte) (metric *common.Metric, err error)

func StatsListener(listen_addr, prefix_internal string, output *common.Output) {
	Listener(listen_addr, prefix_internal, output, ParseLine2)
}

func ArchiveStatsListener(listen_addr, prefix_internal string, output *common.Output) {
	Listener(listen_addr, prefix_internal, output, ParseArchiveLine)
}

// Listener receives packets from the udp buffer, parses them and feeds both the Metrics channel
// as well as the metricAmounts channel
func Listener(listen_addr, prefix_internal string, output *common.Output, parse parseLineFunc) {
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

		for _, p := range ParseMessage(message[:n], prefix_internal, output, parse) {
			output.Metrics <- p
			output.MetricAmounts <- common.MetricAmount{p.Bucket, p.Sampling}
		}
	}
}
