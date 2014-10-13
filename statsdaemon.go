package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/tv42/topic"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/counter"
	"github.com/vimeo/statsdaemon/metrics2"
	"github.com/vimeo/statsdaemon/timer"
	"github.com/vimeo/statsdaemon/udp"
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
)

var signalchan chan os.Signal

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
	listen_addr           = config.String("listen_addr", ":8125")
	listen_archive_addr   = config.String("listen_archive_addr", ":8124")
	admin_addr            = config.String("admin_addr", ":8126")
	graphite_addr         = config.String("graphite_addr", "127.0.0.1:2003")
	flushInterval         = config.Int("flush_interval", 10)
	instance              = config.String("instance", "null")
	prefix_rates          = config.String("prefix_rates", "stats.")
	prefix_timers         = config.String("prefix_timers", "stats.timers.")
	prefix_gauges         = config.String("prefix_gauges", "stats.gauges.")
	percentile_thresholds = config.String("percentile_thresholds", "")
	percentThreshold      = Percentiles{}
	max_timers_per_s      = config.Uint64("max_timers_per_s", 1000)

	debug       = flag.Bool("debug", false, "log outgoing metrics, bad lines, and received admin commands")
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
	Metrics             = make(chan *common.Metric, MAX_UNPROCESSED_PACKETS)
	metricAmounts       = make(chan common.MetricAmount)
	metricStatsRequests = make(chan metricsStatsReq)
	counters            = make(map[string]float64)
	gauges              = make(map[string]float64)
	timers              = make(map[string]timer.Data)
	prefix_internal     string
	valid_lines         = topic.New()
	invalid_lines       = topic.New()
	// currently only used for flush
	events = topic.New()
)

// metricsMonitor basically guards the metrics datastructures.
// it typically receives metrics on the Metrics channel but also responds to
// external signals and every flushInterval, computes and flushes the data
func metricsMonitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				fmt.Printf("!! Caught signal %s... shutting down\n", sig)
				if err := submit(time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				return
			default:
				fmt.Printf("unknown signal %s, ignoring\n", sig)
			}
		case <-ticker.C:
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
			events.Broadcast <- "flush"
		case s := <-Metrics:
			var name string
			if s.Modifier == "ms" {
				timer.Add(timers, s)
				name = "timer"
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = s.Value
				name = "gauge"
			} else {
				counter.Add(counters, s)
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

// instrument wraps around a processing function, and makes sure we track the number of metrics and duration of the call,
// which it flushes as metrics2.0 metrics to the outgoing buffer.
func instrument(fun processFn, buffer *bytes.Buffer, now int64, pctls Percentiles, name string) (num int64) {
	time_start := time.Now()
	num = fun(buffer, now, pctls)
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	fmt.Fprintf(buffer, "%sstatsd_type=%s.target_type=gauge.type=calculation.unit=ms %f %d\n", prefix_internal, name, duration_ms, now)
	fmt.Fprintf(buffer, "%sdirection=out.statsd_type=%s.target_type=rate.unit=Metricps %f %d\n", prefix_internal, name, float64(num)/float64(*flushInterval), now)
	return
}

// submit basically invokes the processing function (instrumented) and tries to buffer to graphite
func submit(deadline time.Time) error {
	var buffer bytes.Buffer

	now := time.Now().Unix()

	// TODO: in future, buffer up data (with a TTL/max size) and submit later
	client, err := net.Dial("tcp", *graphite_addr)
	if err != nil {
		processCounters(&buffer, now, percentThreshold)
		processGauges(&buffer, now, percentThreshold)
		processTimers(&buffer, now, percentThreshold)
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphite_addr, err.Error())
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		errmsg := fmt.Sprintf("could not set deadline - %s", err.Error())
		return errors.New(errmsg)
	}
	instrument(processCounters, &buffer, now, percentThreshold, "counter")
	instrument(processGauges, &buffer, now, percentThreshold, "gauge")
	instrument(processTimers, &buffer, now, percentThreshold, "timer")

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
	if *debug {
		log.Println("submit() successfully finished")
	}

	return nil
}

// processCounters computes the outbound metrics for counters, puts them in the buffer
// and clears the datastructure
func processCounters(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for s, c := range counters {
		v := c / float64(*flushInterval)
		fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Derive_Count(s, *prefix_rates), v, now)
		num++
	}
	counters = make(map[string]float64)
	return num
}

// processGauges puts gauges in the outbound buffer and deactivates the gauges in the datastructure
func processGauges(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for g, c := range gauges {
		if c == math.MaxUint64 {
			continue
		}
		fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Gauge(g, *prefix_gauges), c, now)
		gauges[g] = math.MaxUint64
		num++
	}
	return num
}

// processTimers computes the outbound metrics for timers, puts them in the buffer
// and deactivates their entries in the datastructure
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

				var pctstr string
				var fn func(metric_in, prefix, percentile string) string
				if pct.float >= 0 {
					pctstr = pct.str
					fn = metrics2.Upper
				} else {
					pctstr = pct.str[1:]
					fn = metrics2.Lower
				}
				fmt.Fprintf(buffer, "%s %f %d\n", fn(u, *prefix_timers, pctstr), maxAtThreshold, now)
				fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Mean(u, *prefix_timers, pctstr), mean_pct, now)
				fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Sum(u, *prefix_timers, pctstr), sum_pct, now)
			}

			var z timer.Float64Slice
			timers[u] = timer.Data{z, 0}

			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Mean(u, *prefix_timers, ""), mean, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Median(u, *prefix_timers, ""), median, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Std(u, *prefix_timers, ""), stddev, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Sum(u, *prefix_timers, ""), sum, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Upper(u, *prefix_timers, ""), max, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Lower(u, *prefix_timers, ""), min, now)
			fmt.Fprintf(buffer, "%s %d %d\n", metrics2.Count_Pckt(u, *prefix_timers), count, now)
			fmt.Fprintf(buffer, "%s %f %d\n", metrics2.Rate_Pckt(u, *prefix_timers), count_ps, now)
		}
	}
	return num
}

// Amounts is a datastructure to track numbers of packets, in particular:
// * Submitted is "triggered" inside statsd client libs, not necessarily sent
// * Seen is the amount we see. I.e. after sampling, network loss and udp packet drops
type Amounts struct {
	Submitted uint64
	Seen      uint64
}

// metricsStatsMonitor basically maintains and guards the Amounts datastructures, and pulls
// information out of it to satisfy requests.
// we keep 2 10-second buffers, so that every 10 seconds we can restart filling one of them
// (by reading from the metricAmounts channel),
// while having another so that at any time we have at least 10 seconds worth of data (upto 20s)
// upon incoming requests we use the "old" buffer and the new one for the timeperiod it applies to.
// (this way we have the absolute latest information)
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
		case s_a := <-metricAmounts:
			el, ok := (*cur_counts)[s_a.Bucket]
			if ok {
				el.Seen += 1
				el.Submitted += uint64(1 / s_a.Sampling)
			} else {
				(*cur_counts)[s_a.Bucket] = &Amounts{uint64(1 / s_a.Sampling), 1}
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
				submitted_per_s := float64(submitted) / interval
				// submitted (at source) per second * ideal_sample_rate should be ~= *max_timers_per_s
				ideal_sample_rate := float64(1)
				if uint64(submitted_per_s) > *max_timers_per_s {
					ideal_sample_rate = float64(*max_timers_per_s) / submitted_per_s
				}
				fmt.Fprintf(&resp, "%s %f %f\n", bucket, ideal_sample_rate, submitted_per_s)
				// this needs to be less realtime, so for simplicity (and performance?) we just use the prev 10s bucket.
			case "metric_stats":
				for bucket, el := range *prev_counts {
					fmt.Fprintf(&resp, "%s %f %f\n", bucket, float64(el.Submitted)/10, float64(el.Seen)/10)
				}
			}

			go handleApiRequest(*req.Conn, resp)
		}
	}
}

func writeHelp(conn net.Conn) {
	help := `
commands:
    help                        show this menu
    sample_rate <metric key>    for given metric, show:
                                <key> <ideal sample rate> <Pckt/s sent (estim)>
    metric_stats                in the past 10s interval, for every metric show:
                                <key> <Pckt/s sent (estim)> <Pckt/s received>
    peek_valid                  stream all valid lines seen in real time
                                until you disconnect or can't keep up.
    peek_invalid                stream all invalid lines seen in real time
                                until you disconnect or can't keep up.
    wait_flush                  after the next flush, writes 'flush' and closes connection.
                                this is convenient to restart statsdaemon
                                with a minimal loss of data like so:
                                nc localhost 8126 <<< wait_flush && /sbin/restart statsdaemon


`
	conn.Write([]byte(help))
}

// handleApiRequest handles one or more api requests over the admin interface, to the extent it can.
// some operations need to be performed by a Monitor, so we write the request into a channel along with
// the connection.  the monitor will handle the request when it gets to it, and invoke this function again
// so we can resume handling a request.
func handleApiRequest(conn net.Conn, write_first bytes.Buffer) {
	write_first.WriteTo(conn)
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("[api] read eof. closing")
			} else {
				fmt.Println("[api] Error reading:", err.Error())
			}
			conn.Close()
			break
		}
		clean_cmd := strings.TrimSpace(string(buf[:n]))
		command := strings.Split(clean_cmd, " ")
		if *debug {
			log.Println("[api] received command: '" + clean_cmd + "'")
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
		case "peek_invalid":
			consumer := make(chan interface{}, 100)
			invalid_lines.Register(consumer)
			conn.(*net.TCPConn).SetNoDelay(false)
			for line := range consumer {
				conn.Write(line.([]byte))
				conn.Write([]byte("\n"))
			}
			conn.(*net.TCPConn).SetNoDelay(true)
		case "peek_valid":
			consumer := make(chan interface{}, 100)
			valid_lines.Register(consumer)
			conn.(*net.TCPConn).SetNoDelay(false)
			for line := range consumer {
				conn.Write(line.([]byte))
				conn.Write([]byte("\n"))
			}
			conn.(*net.TCPConn).SetNoDelay(true)
		case "wait_flush":
			consumer := make(chan interface{}, 10)
			events.Register(consumer)
			ev := <-consumer
			conn.Write([]byte(ev.(string)))
			conn.Write([]byte("\n"))
			conn.Close()
			break
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
func expand_cfg_vars(in string) (out string) {
	switch in {
	case "HOST":
		hostname, _ := os.Hostname()
		// in case hostname is an fqdn or has dots, only take first part
		parts := strings.SplitN(hostname, ".", 2)
		return parts[0]
	default:
		return ""
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
	pcts := strings.Split(*percentile_thresholds, ",")
	for _, pct := range pcts {
		percentThreshold.Set(pct)
	}
	inst := os.Expand(*instance, expand_cfg_vars)
	if inst == "" {
		inst = "null"
	}
	prefix_internal = "service=statsdaemon.instance=" + inst + "."
	log.Printf("statsdaemon instance '%s' starting\n", inst)

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan)
	if *debug {
		consumer := make(chan interface{}, 100)
		invalid_lines.Register(consumer)
		go func() {
			for line := range consumer {
				log.Printf("invalid line '%s'\n", line)
			}
		}()
	}
	output := &common.Output{Metrics, metricAmounts, valid_lines, invalid_lines}
	go udp.StatsListener(*listen_addr, prefix_internal, output)
	go udp.ArchiveStatsListener(*listen_archive_addr, prefix_internal, output)
	go adminListener()
	go metricStatsMonitor()
	metricsMonitor()
}
