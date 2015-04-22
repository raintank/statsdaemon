package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/tv42/topic"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/counters"
	"github.com/vimeo/statsdaemon/gauges"
	"github.com/vimeo/statsdaemon/timers"
	"github.com/vimeo/statsdaemon/udp"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/stvp/go-toml-config"
	"net/http"
	_ "net/http/pprof"
)

const (
	VERSION = "0.5.2-alpha"
	// number of packets we can read out of udp buffer without processing them
	// this number shouldn't be high because metricsMonitor()
	// should be able to continuously read in metrics with barely any blocking
	// keep in mind that one metric is about 30 to 100 bytes of memory, so this is peanuts
	MAX_UNPROCESSED_PACKETS = 1000
)

var signalchan chan os.Signal

var (
	listen_addr           = config.String("listen_addr", ":8125")
	admin_addr            = config.String("admin_addr", ":8126")
	profile_addr          = config.String("profile_addr", ":6060")
	graphite_addr         = config.String("graphite_addr", "127.0.0.1:2003")
	flushInterval         = config.Int("flush_interval", 10)
	processes             = config.Int("processes", 1)
	instance              = config.String("instance", "null")
	prefix_rates          = config.String("prefix_rates", "stats.")
	prefix_timers         = config.String("prefix_timers", "stats.timers.")
	prefix_gauges         = config.String("prefix_gauges", "stats.gauges.")
	percentile_thresholds = config.String("percentile_thresholds", "")
	percentThreshold      *timers.Percentiles
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
	ticker := getAlignedTicker(period)

	var c *counters.Counters
	var g *gauges.Gauges
	var t *timers.Timers

	initializeCounters := func() {
		c = counters.New(*prefix_rates)
		g = gauges.New(*prefix_gauges)
		t = timers.New(*prefix_timers, *percentThreshold)
		for _, name := range []string{"timer", "gauge", "counter"} {
			c.Add(&common.Metric{
				Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_%s.target_type_is_count.unit_is_Metric", prefix_internal, name),
				Sampling: 1,
			})
		}
	}
	initializeCounters()
	for {
		select {
		case sig := <-signalchan:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				fmt.Printf("!! Caught signal %s... shutting down\n", sig)
				if err := submit(c, g, t, time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				return
			default:
				fmt.Printf("unknown signal %s, ignoring\n", sig)
			}
		case <-ticker.C:
			go func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers) {
				if err := submit(c, g, t, time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				events.Broadcast <- "flush"
			}(c, g, t)
			initializeCounters()
			ticker = getAlignedTicker(period)
		case s := <-Metrics:
			var name string
			if s.Modifier == "ms" {
				t.Add(s)
				name = "timer"
			} else if s.Modifier == "g" {
				g.Add(s)
				name = "gauge"
			} else {
				c.Add(s)
				name = "counter"
			}
			c.Add(&common.Metric{
				Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_%s.target_type_is_count.unit_is_Metric", prefix_internal, name),
				Value:    1,
				Sampling: 1,
			})
		}
	}
}

type statsdType interface {
	Add(metric *common.Metric)
	Process(buffer *bytes.Buffer, now int64, interval int) int64
}

// instrument wraps around a processing function, and makes sure we track the number of metrics and duration of the call,
// which it flushes as metrics2.0 metrics to the outgoing buffer.
func instrument(st statsdType, buffer *bytes.Buffer, now int64, name string) (num int64) {
	time_start := time.Now()
	num = st.Process(buffer, now, *flushInterval)
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	fmt.Fprintf(buffer, "%sstatsd_type_is_%s.target_type_is_gauge.type_is_calculation.unit_is_ms %f %d\n", prefix_internal, name, duration_ms, now)
	fmt.Fprintf(buffer, "%sdirection_is_out.statsd_type_is_%s.target_type_is_rate.unit_is_Metricps %f %d\n", prefix_internal, name, float64(num)/float64(*flushInterval), now)
	return
}

// submit basically invokes the processing function (instrumented) and tries to buffer to graphite
func submit(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time) error {
	var buffer bytes.Buffer

	now := time.Now().Unix()

	// TODO: in future, buffer up data (with a TTL/max size) and submit later
	client, err := net.Dial("tcp", *graphite_addr)
	if err != nil {
		c.Process(&buffer, now, *flushInterval)
		g.Process(&buffer, now, *flushInterval)
		t.Process(&buffer, now, *flushInterval)
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphite_addr, err.Error())
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		errmsg := fmt.Sprintf("could not set deadline - %s", err.Error())
		return errors.New(errmsg)
	}
	instrument(c, &buffer, now, "counter")
	instrument(g, &buffer, now, "gauge")
	instrument(t, &buffer, now, "timer")

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: WRITING %s", line)
		}
	}

	time_start := time.Now()
	_, err = client.Write(buffer.Bytes())
	if err != nil {
		errmsg := fmt.Sprintf("failed to write stats - %s", err)
		return errors.New(errmsg)
	}
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	if *debug {
		log.Println("submit() successfully finished")
	}

	buffer.Reset()
	fmt.Fprintf(&buffer, "%starget_type_is_gauge.type_is_send.unit_is_ms %f %d\n", prefix_internal, duration_ms, now)
	_, err = client.Write(buffer.Bytes())
	if err != nil {
		errmsg := fmt.Sprintf("failed to write target_type_is_gauge.type_is_send.unit_is_ms - %s", err)
		return errors.New(errmsg)
	}

	return nil
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
	runtime.GOMAXPROCS(*processes)
	var err error
	percentThreshold, err = timers.NewPercentiles(*percentile_thresholds)
	if err != nil {
		log.Fatal(err)
	}
	inst := os.Expand(*instance, expand_cfg_vars)
	if inst == "" {
		inst = "null"
	}
	prefix_internal = "service_is_statsdaemon.instance_is_" + inst + "."
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
	if *profile_addr != "" {
		go func() {
			fmt.Println("Profiling endpoint listening on " + *profile_addr)
			log.Println(http.ListenAndServe(*profile_addr, nil))
		}()
	}
	output := &common.Output{Metrics, metricAmounts, valid_lines, invalid_lines}
	go udp.StatsListener(*listen_addr, prefix_internal, output)
	go adminListener()
	go metricStatsMonitor()
	metricsMonitor()
}
