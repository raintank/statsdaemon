package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/metrics2"
	"github.com/vimeo/statsdaemon/timer"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/stvp/go-toml-config"
)

const (
	VERSION                 = "0.5.2-alpha"
	MAX_UNPROCESSED_PACKETS = 1000
	MAX_UDP_PACKET_SIZE     = 512
)

var signalchan chan os.Signal

// an amount of 1 per instance is imlpied
type metricAmount struct {
	Bucket   string
	Sampling float32
}

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

var (
	listen_addr          = config.String("listen_addr", ":8125")
	admin_addr           = config.String("admin_addr", ":8126")
	graphite_addr        = config.String("graphite_addr", "127.0.0.1:2003")
	flushInterval        = config.Int("flush_interval", 10)
	instance             = config.String("instance", "default")
	prefix_rates         = config.String("prefix_rates", "stats.")
	prefix_timers        = config.String("prefix_timers", "stats.timers.")
	prefix_gauges        = config.String("prefix_gauges", "stats.gauges.")
	percentile_tresholds = config.String("percentile_tresholds", "")
	percentThreshold     = Percentiles{}
	max_timers_per_s     = config.Uint64("max_timers_per_s", 1000)

	debug       = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion = flag.Bool("version", false, "print version string")
	config_file = flag.String("config_file", "/etc/statsdaemon.ini", "config file location")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile  = flag.String("memprofile", "", "write memory profile to this file")
)

type metricsStatsReq struct {
	Command []string
	Conn    *net.Conn
}

var (
	Metrics               = make(chan *common.Metric, MAX_UNPROCESSED_PACKETS)
	metricAmountCollector = make(chan metricAmount)
	metricStatsRequests   = make(chan metricsStatsReq)
	counters              = make(map[string]float64)
	gauges                = make(map[string]float64)
	timers                = make(map[string]timer.Data)
	prefix_internal       string
)

func metricsMonitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				fmt.Printf("!! Caught signal %d... shutting down\n", sig)
				if err := submit(time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				return
			default:
				fmt.Printf("unknown signal %d, ignoring\n", sig)
			}
		case <-ticker.C:
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
		case s := <-Metrics:
			var name string
			if s.Modifier == "ms" {
				timer.Add(timers, s)
				name = "timer"
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = s.Value
				name = "gauge"
			} else {
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += s.Value * float64(1/s.Sampling)
				name = "counter"
			}
			k := fmt.Sprintf("%sdirection=in.statsd_type=%s.target_type=count.unit=Metric", prefix_internal, name)
			_, ok := counters[k]
			if !ok {
				counters[k] = 1
			} else {
				counters[k] += 1
			}
		}
	}
}

type processFn func(*bytes.Buffer, int64, Percentiles) int64

func instrument(fun processFn, buffer *bytes.Buffer, now int64, pctls Percentiles, name string) (num int64) {
	time_start := time.Now()
	num = fun(buffer, now, pctls)
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	fmt.Fprintf(buffer, "%s%sstatsd_type=%s.target_type=gauge.type=calculation.unit=ms %f %d\n", *prefix_gauges, prefix_internal, name, duration_ms, now)
	fmt.Fprintf(buffer, "%s%sdirection=out.statsd_type=%s.target_type=rate.unit=Metricps %f %d\n", *prefix_rates, prefix_internal, name, float64(num)/float64(*flushInterval), now)
	return
}

func submit(deadline time.Time) error {
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()

	client, err := net.Dial("tcp", *graphite_addr)
	if err != nil {
		if *debug {
			log.Printf("WARNING: resetting counters when in debug mode")
			processCounters(&buffer, now, percentThreshold)
			processGauges(&buffer, now, percentThreshold)
			processTimers(&buffer, now, percentThreshold)
		}
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphite_addr, err)
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		errmsg := fmt.Sprintf("could not set deadline:", err)
		return errors.New(errmsg)
	}
	num += instrument(processCounters, &buffer, now, percentThreshold, "counter")
	num += instrument(processGauges, &buffer, now, percentThreshold, "gauge")
	num += instrument(processTimers, &buffer, now, percentThreshold, "timer")
	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: WRITING %s", line)
		}
	}

	_, err = client.Write(buffer.Bytes())
	if err != nil {
		errmsg := fmt.Sprintf("failed to write stats - %s", err)
		return errors.New(errmsg)
	}

	//fmt.Println("end of submit")
	//fmt.Fprintf(&buffer, ...
	return nil
}

func processCounters(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for s, c := range counters {
		v := c / float64(*flushInterval)
		s = metrics2.Derive(s)
		fmt.Fprintf(buffer, "%s%s %f %d\n", *prefix_rates, s, v, now)
		num++
		delete(counters, s)
	}
	//counters = make(map[string]float64) this should be better than deleting every single entry
	return num
}

func processGauges(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for g, c := range gauges {
		if c == math.MaxUint64 {
			continue
		}
		fmt.Fprintf(buffer, "%s%s %f %d\n", *prefix_gauges, g, c, now)
		gauges[g] = math.MaxUint64
		num++
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
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
	for u, t := range timers {
		if len(t.Points) > 0 {
			seen := len(t.Points)
			count := t.Amount_submitted
			count_ps := float64(count) / float64(*flushInterval)
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
			var cumulativeValues timer.Float64Slice
			cumulativeValues = make(timer.Float64Slice, seen, seen)
			cumulativeValues[0] = t.Points[0]
			for i := 1; i < seen; i++ {
				cumulativeValues[i] = t.Points[i] + cumulativeValues[i-1]
			}

			maxAtThreshold := max
			sum_pct := sum
			mean_pct := mean

			for _, pct := range pctls {

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

				var tmpl string
				var pctstr string
				if pct.float >= 0 {
					tmpl = "%s%s.upper_%s %f %d\n"
					pctstr = pct.str
				} else {
					tmpl = "%s%s.lower_%s %f %d\n"
					pctstr = pct.str[1:]
				}
				fmt.Fprintf(buffer, tmpl, *prefix_timers, u, pctstr, maxAtThreshold, now)
				fmt.Fprintf(buffer, "%s%s.mean_%s %f %d\n", *prefix_timers, u, pctstr, mean_pct, now)
				fmt.Fprintf(buffer, "%s%s.sum_%s %f %d\n", *prefix_timers, u, pctstr, sum_pct, now)
			}

			var z timer.Float64Slice
			timers[u] = timer.Data{z, 0}

			fmt.Fprintf(buffer, "%s%s.mean %f %d\n", *prefix_timers, u, mean, now)
			fmt.Fprintf(buffer, "%s%s.median %f %d\n", *prefix_timers, u, median, now)
			fmt.Fprintf(buffer, "%s%s.std %f %d\n", *prefix_timers, u, stddev, now)
			fmt.Fprintf(buffer, "%s%s.sum %f %d\n", *prefix_timers, u, sum, now)
			fmt.Fprintf(buffer, "%s%s.upper %f %d\n", *prefix_timers, u, max, now)
			fmt.Fprintf(buffer, "%s%s.lower %f %d\n", *prefix_timers, u, min, now)
			fmt.Fprintf(buffer, "%s%s.count %d %d\n", *prefix_timers, u, count, now)
			fmt.Fprintf(buffer, "%s%s.count_ps %f %d\n", *prefix_timers, u, count_ps, now)
		}
	}
	return num
}

func parseLine(line []byte) (metric *common.Metric, valid bool) {
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

func parseMessage(data []byte) []*common.Metric {
	var output []*common.Metric
	for _, line := range bytes.Split(data, []byte("\n")) {
		metric, valid := parseLine(line)
		if !valid {
			if *debug {
				log.Printf("invalid line '%s'\n", line)
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

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *listen_addr)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()

	message := make([]byte, MAX_UDP_PACKET_SIZE)
	for {
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		for _, p := range parseMessage(message[:n]) {
			Metrics <- p
			metricAmountCollector <- metricAmount{p.Bucket, p.Sampling}
		}
	}
}

// submitted is "triggered" inside statsd client libs, not necessarily sent
// after sampling, network loss and udp packet drops, the amount we see is Seen
type Amounts struct {
	Submitted uint64
	Seen      uint64
}

func metricStatsMonitor() {
	period := 10 * time.Second
	ticker := time.NewTicker(period)
	// use two maps so we always have enough data shortly after we start a new period
	// counts would be too low and/or too inaccurate otherwise
	_countsA := make(map[string]*Amounts)
	_countsB := make(map[string]*Amounts)
	cur_counts := &_countsA
	prev_counts := &_countsB
	var swap_ts time.Time
	for {
		select {
		case <-ticker.C:
			prev_counts = cur_counts
			new_counts := make(map[string]*Amounts)
			cur_counts = &new_counts
			swap_ts = time.Now()
		case s_a := <-metricAmountCollector:
			el, ok := (*cur_counts)[s_a.Bucket]
			if ok {
				el.Seen += 1
				el.Submitted += uint64(1 / s_a.Sampling)
			} else {
				(*cur_counts)[s_a.Bucket] = &Amounts{1, uint64(1 / s_a.Sampling)}
			}
		case req := <-metricStatsRequests:
			current_ts := time.Now()
			interval := current_ts.Sub(swap_ts).Seconds() + 10
			var resp bytes.Buffer
			switch req.Command[0] {
			case "sample_rate":
				bucket := req.Command[1]
				submitted := uint64(0)
				el, ok := (*cur_counts)[bucket]
				if ok {
					submitted += el.Submitted
				}
				el, ok = (*prev_counts)[bucket]
				if ok {
					submitted += el.Submitted
				}
				submitted_per_s := submitted / uint64(interval)
				// submitted (at source) per second * ideal_sample_rate should be ~= *max_timers_per_s
				ideal_sample_rate := float32(1)
				if submitted_per_s > *max_timers_per_s {
					ideal_sample_rate = float32(*max_timers_per_s) / float32(submitted_per_s)
				}
				fmt.Fprintf(&resp, "%s %f %d\n", bucket, ideal_sample_rate, submitted_per_s)
			case "metric_stats":
				for bucket, el := range *prev_counts {
					fmt.Fprintf(&resp, "%s %d %d\n", bucket, el.Submitted/10, el.Seen/10)
				}
			}

			go handleApiRequest(*req.Conn, resp)
		}
	}
}

func writeHelp(conn net.Conn) {
	help := `
    commands:
        sample_rate <metric key>         for given metric, show:
                                         <key> <ideal sample rate> <Pckt/s sent (estim)>
        help                             show this menu
        metric_stats                     in the past 10s interval, for every metric show:
                                         <key> <Pckt/s sent (estim)> <Pckt/s received>

`
	conn.Write([]byte(help))
}

func handleApiRequest(conn net.Conn, write_first bytes.Buffer) {
	write_first.WriteTo(conn)
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("read eof. closing")
				conn.Close()
				break
			} else {
				fmt.Println("Error reading:", err.Error())
			}
		}
		clean_cmd := strings.TrimSpace(string(buf[:n]))
		command := strings.Split(clean_cmd, " ")
		if *debug {
			fmt.Println("received command: '" + clean_cmd + "'")
		}
		switch command[0] {
		case "sample_rate":
			if len(command) != 2 {
				conn.Write([]byte("invalid request\n"))
				writeHelp(conn)
				continue
			}
			metricStatsRequests <- metricsStatsReq{command, &conn}
			return
		case "metric_stats":
			if len(command) != 1 {
				conn.Write([]byte("invalid request\n"))
				writeHelp(conn)
				continue
			}
			metricStatsRequests <- metricsStatsReq{command, &conn}
			return
		case "help":
			writeHelp(conn)
			continue
		default:
			conn.Write([]byte("unknown command\n"))
			writeHelp(conn)
		}
	}
}
func adminListener() {
	l, err := net.Listen("tcp", *admin_addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on " + *admin_addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handleApiRequest(conn, bytes.Buffer{})
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}
	config.Parse(*config_file)
	pcts := strings.Split(*percentile_tresholds, ",")
	for _, pct := range pcts {
		percentThreshold.Set(pct)
	}
	prefix_internal = "service=statsdaemon.instance=" + *instance + "."

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan)

	go udpListener()
	go adminListener()
	go metricStatsMonitor()
	metricsMonitor()
}
