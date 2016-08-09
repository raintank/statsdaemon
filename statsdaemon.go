package statsdaemon

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/tv42/topic"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/counters"
	"github.com/vimeo/statsdaemon/gauges"
	"github.com/vimeo/statsdaemon/ticker"
	"github.com/vimeo/statsdaemon/timers"
	"github.com/vimeo/statsdaemon/udp"
)

type metricsStatsReq struct {
	Command []string
	Conn    *net.Conn
}

type SubmitFunc func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time)

type StatsDaemon struct {
	instance            string
	listen_addr         string
	admin_addr          string
	graphite_addr       string
	prefix              string
	prefix_rates        string
	prefix_counters     string
	prefix_timers       string
	prefix_gauges       string
	legacy_namespace    bool
	flush_rates         bool
	flush_counts        bool
	pct                 timers.Percentiles
	flushInterval       int
	max_unprocessed     int
	max_timers_per_s    uint64
	signalchan          chan os.Signal
	Metrics             chan []*common.Metric
	metricAmounts       chan []*common.Metric
	metricStatsRequests chan metricsStatsReq
	valid_lines         *topic.Topic
	Invalid_lines       *topic.Topic
	events              *topic.Topic
	debug               bool
	Clock               clock.Clock
	submitFunc          SubmitFunc
	graphiteQueue       chan []byte
}

func New(instance, prefix_rates, prefix_timers, prefix_gauges, prefix_counters string, pct timers.Percentiles, flushInterval, max_unprocessed int, max_timers_per_s uint64, signalchan chan os.Signal, debug, legacy_namespace, flush_rates, flush_counts bool) *StatsDaemon {
	return &StatsDaemon{
		instance,
		"",
		"",
		"",
		"service_is_statsdaemon.instance_is_" + instance + ".",
		prefix_rates,
		prefix_counters,
		prefix_timers,
		prefix_gauges,
		legacy_namespace,
		flush_rates,
		flush_counts,
		pct,
		flushInterval,
		max_unprocessed,
		max_timers_per_s,
		signalchan,
		make(chan []*common.Metric, max_unprocessed),
		make(chan []*common.Metric, max_unprocessed),
		make(chan metricsStatsReq),
		topic.New(),
		topic.New(),
		topic.New(),
		debug,
		nil,
		nil,
		nil,
	}
}

// start statsdaemon instance with standard network daemon behaviors
func (s *StatsDaemon) Run(listen_addr, admin_addr, graphite_addr string) {
	s.listen_addr = listen_addr
	s.admin_addr = admin_addr
	s.graphite_addr = graphite_addr
	s.submitFunc = s.GraphiteQueue
	s.Clock = clock.New()
	s.graphiteQueue = make(chan []byte, 1000)
	log.Printf("statsdaemon instance '%s' starting\n", s.instance)
	output := &common.Output{s.Metrics, s.metricAmounts, s.valid_lines, s.Invalid_lines}
	go udp.StatsListener(s.listen_addr, s.prefix, output) // set up udp listener that writes messages to output's channels (i.e. s's channels)
	go s.adminListener()                                  // tcp admin_addr to handle requests
	go s.metricStatsMonitor()                             // handles requests fired by telnet api
	go s.graphiteWriter()                                 // writes to graphite in the background
	s.metricsMonitor()                                    // takes data from s.Metrics and puts them in the guage/timers/etc objects. pointers guarded by select. also listens for signals.
}

// start statsdaemon instance, only processing incoming metrics from the channel, and flushing
// no admin listener
// up to you to write to Metrics and metricAmounts channels, and set submitFunc, and set the clock

func (s *StatsDaemon) RunBare() {
	log.Printf("statsdaemon instance '%s' starting\n", s.instance)
	go s.metricStatsMonitor()
	s.metricsMonitor()
}

// metricsMonitor basically guards the metrics datastructures.
// it typically receives metrics on the Metrics channel but also responds to
// external signals and every flushInterval, computes and flushes the data
func (s *StatsDaemon) metricsMonitor() {
	period := time.Duration(s.flushInterval) * time.Second
	tick := ticker.GetAlignedTicker(s.Clock, period)

	var c *counters.Counters
	var g *gauges.Gauges
	var t *timers.Timers
	oneCounter := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_counter.target_type_is_count.unit_is_Metric", s.prefix),
		Value:    1,
		Sampling: 1,
	}
	oneGauge := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_gauge.target_type_is_count.unit_is_Metric", s.prefix),
		Value:    1,
		Sampling: 1,
	}
	oneTimer := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_timer.target_type_is_count.unit_is_Metric", s.prefix),
		Value:    1,
		Sampling: 1,
	}

	initializeCounters := func() {
		c = counters.New(s.prefix_rates, s.prefix_counters, s.legacy_namespace, s.flush_rates, s.flush_counts)
		g = gauges.New(s.prefix_gauges)
		t = timers.New(s.prefix_timers, s.pct)
		for _, name := range []string{"timer", "gauge", "counter"} {
			c.Add(&common.Metric{
				Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_%s.target_type_is_count.unit_is_Metric", s.prefix, name),
				Sampling: 1,
			})
		}
	}
	initializeCounters()
	for {
		select {
		case sig := <-s.signalchan:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				fmt.Printf("!! Caught signal %s... shutting down\n", sig)
				s.submitFunc(c, g, t, s.Clock.Now().Add(period))
				return
			default:
				fmt.Printf("unknown signal %s, ignoring\n", sig)
			}
		case <-tick.C:
			go func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers) {
				s.submitFunc(c, g, t, s.Clock.Now().Add(period))
				s.events.Broadcast <- "flush"
			}(c, g, t)
			initializeCounters()
			tick = ticker.GetAlignedTicker(s.Clock, period)
		case metrics := <-s.Metrics:
			for _, m := range metrics {
				if m.Modifier == "ms" {
					t.Add(m)
					c.Add(oneTimer)
				} else if m.Modifier == "g" {
					g.Add(m)
					c.Add(oneGauge)
				} else {
					c.Add(m)
					c.Add(oneCounter)
				}
			}
		}
	}
}

type statsdType interface {
	Add(metric *common.Metric)
	Process(buf []byte, now int64, interval int) ([]byte, int64)
}

// instrument wraps around a processing function, and makes sure we track the number of metrics and duration of the call,
// which it flushes as metrics2.0 metrics to the outgoing buffer.
func (s *StatsDaemon) instrument(st statsdType, buf []byte, now int64, name string) ([]byte, int64) {
	time_start := s.Clock.Now()
	buf, num := st.Process(buf, now, s.flushInterval)
	time_end := s.Clock.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	buf = common.WriteFloat64(buf, []byte(fmt.Sprintf("%sstatsd_type_is_%s.target_type_is_gauge.type_is_calculation.unit_is_ms", s.prefix, name)), duration_ms, now)
	buf = common.WriteFloat64(buf, []byte(fmt.Sprintf("%sdirection_is_out.statsd_type_is_%s.target_type_is_rate.unit_is_Metricps", s.prefix, name)), float64(num)/float64(s.flushInterval), now)
	return buf, num
}

// graphiteWriter is the background workers that connects to graphite and submits all pending data to it
// TODO: conn.Write() returns no error for a while when the remote endpoint is down, the reconnect happens with a delay
func (s *StatsDaemon) graphiteWriter() {
	lock := &sync.Mutex{}
	connectTicker := s.Clock.Tick(2 * time.Second)
	var conn net.Conn
	var err error
	go func() {
		for range connectTicker {
			lock.Lock()
			if conn == nil {
				conn, err = net.Dial("tcp", s.graphite_addr)
				if err == nil {
					log.Printf("now connected to %s", s.graphite_addr)
				} else {
					log.Printf("WARN: dialing %s failed: %s. will retry", s.graphite_addr, err.Error())
				}
			}
			lock.Unlock()
		}
	}()
	for buf := range s.graphiteQueue {
		lock.Lock()
		haveConn := (conn != nil)
		lock.Unlock()
		for !haveConn {
			s.Clock.Sleep(time.Second)
			lock.Lock()
			haveConn = (conn != nil)
			lock.Unlock()
		}
		if s.debug {
			for _, line := range bytes.Split(buf, []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				log.Printf("DEBUG: WRITING %s", line)
			}
		}
		ok := false
		var duration float64
		var pre time.Time
		for !ok {
			pre = s.Clock.Now()
			lock.Lock()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
				duration = float64(s.Clock.Now().Sub(pre).Nanoseconds()) / float64(1000000)
				if s.debug {
					log.Println("DEBUG: wrote metrics payload to graphite!")
				}
			} else {
				log.Printf("failed to write to graphite: %s (took %s). will retry...", err, s.Clock.Now().Sub(pre))
				conn.Close()
				conn = nil
				haveConn = false
			}
			lock.Unlock()
			for !ok && !haveConn {
				s.Clock.Sleep(2 * time.Second)
				lock.Lock()
				haveConn = (conn != nil)
				lock.Unlock()
			}
		}
		buf = buf[:0]
		buf = common.WriteFloat64(buf, []byte(fmt.Sprintf("%starget_type_is_gauge.type_is_send.unit_is_ms", s.prefix)), duration, pre.Unix())
		ok = false
		for !ok {
			lock.Lock()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
				if s.debug {
					log.Println("DEBUG: wrote sendtime to graphite!")
				}
			} else {
				log.Printf("failed to write target_type_is_gauge.type_is_send.unit_is_ms: %s. will retry...", err)
				conn.Close()
				conn = nil
				haveConn = false
			}
			lock.Unlock()
			for !ok && !haveConn {
				s.Clock.Sleep(2 * time.Second)
				lock.Lock()
				haveConn = (conn != nil)
				lock.Unlock()
			}
		}
	}
	lock.Lock()
	if conn != nil {
		conn.Close()
	}
	lock.Unlock()
}

// GraphiteQuepue invokes the processing function (instrumented) and enqueues data for writing to graphite
func (s *StatsDaemon) GraphiteQueue(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time) {
	buf := make([]byte, 0)

	now := s.Clock.Now().Unix()
	buf, _ = s.instrument(c, buf, now, "counter")
	buf, _ = s.instrument(g, buf, now, "gauge")
	buf, _ = s.instrument(t, buf, now, "timer")
	s.graphiteQueue <- buf
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
func (s *StatsDaemon) metricStatsMonitor() {
	period := 10 * time.Second
	tick := s.Clock.Ticker(period)
	// use two maps so we always have enough data shortly after we start a new period
	// counts would be too low and/or too inaccurate otherwise
	_countsA := make(map[string]Amounts)
	_countsB := make(map[string]Amounts)
	cur_counts := &_countsA
	prev_counts := &_countsB
	var swap_ts time.Time
	for {
		select {
		case <-tick.C:
			prev_counts = cur_counts
			new_counts := make(map[string]Amounts)
			cur_counts = &new_counts
			swap_ts = s.Clock.Now()
		case metrics := <-s.metricAmounts:
			for _, metric := range metrics {
				el, ok := (*cur_counts)[metric.Bucket]
				if ok {
					el.Seen += 1
					el.Submitted += uint64(1 / metric.Sampling)
				} else {
					(*cur_counts)[metric.Bucket] = Amounts{uint64(1 / metric.Sampling), 1}
				}
			}
		case req := <-s.metricStatsRequests:
			current_ts := s.Clock.Now()
			interval := current_ts.Sub(swap_ts).Seconds() + 10
			var buf []byte
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
				// submitted (at source) per second * ideal_sample_rate should be ~= max_timers_per_s
				ideal_sample_rate := float64(1)
				if uint64(submitted_per_s) > s.max_timers_per_s {
					ideal_sample_rate = float64(s.max_timers_per_s) / submitted_per_s
				}
				buf = append(buf, []byte(fmt.Sprintf("%s %f %f\n", bucket, ideal_sample_rate, submitted_per_s))...)
				// this needs to be less realtime, so for simplicity (and performance?) we just use the prev 10s bucket.
			case "metric_stats":
				for bucket, el := range *prev_counts {
					buf = append(buf, []byte(fmt.Sprintf("%s %f %f\n", bucket, float64(el.Submitted)/10, float64(el.Seen)/10))...)
				}
			}

			go s.handleApiRequest(*req.Conn, buf)
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
func (s *StatsDaemon) handleApiRequest(conn net.Conn, write_first []byte) {
	if write_first != nil {
		conn.Write(write_first)
	}
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
		if s.debug {
			log.Println("DEBUG: [api] received command: '" + clean_cmd + "'")
		}
		switch command[0] {
		case "sample_rate":
			if len(command) != 2 {
				conn.Write([]byte("invalid request\n"))
				writeHelp(conn)
				continue
			}
			s.metricStatsRequests <- metricsStatsReq{command, &conn}
			return
		case "metric_stats":
			if len(command) != 1 {
				conn.Write([]byte("invalid request\n"))
				writeHelp(conn)
				continue
			}
			s.metricStatsRequests <- metricsStatsReq{command, &conn}
			return
		case "peek_invalid":
			consumer := make(chan interface{}, 100)
			s.Invalid_lines.Register(consumer)
			conn.(*net.TCPConn).SetNoDelay(false)
			for line := range consumer {
				conn.Write(line.([]byte))
				conn.Write([]byte("\n"))
			}
			conn.(*net.TCPConn).SetNoDelay(true)
		case "peek_valid":
			consumer := make(chan interface{}, 100)
			s.valid_lines.Register(consumer)
			conn.(*net.TCPConn).SetNoDelay(false)
			for line := range consumer {
				conn.Write(line.([]byte))
				conn.Write([]byte("\n"))
			}
			conn.(*net.TCPConn).SetNoDelay(true)
		case "wait_flush":
			consumer := make(chan interface{}, 10)
			s.events.Register(consumer)
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
func (s *StatsDaemon) adminListener() {
	l, err := net.Listen("tcp", s.admin_addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on " + s.admin_addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go s.handleApiRequest(conn, nil)
	}
}
