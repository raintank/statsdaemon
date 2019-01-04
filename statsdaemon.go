package statsdaemon

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/raintank/statsdaemon/common"
	"github.com/raintank/statsdaemon/out"
	"github.com/raintank/statsdaemon/ticker"
	"github.com/raintank/statsdaemon/udp"
	log "github.com/sirupsen/logrus"
	"github.com/tv42/topic"
)

type metricsStatsReq struct {
	Command []string
	Conn    *net.Conn
}

type SubmitFunc func(c *out.Counters, g *out.Gauges, t *out.Timers, deadline time.Time)
type StatsDaemon struct {
	instance string

	fmt              out.Formatter
	flush_rates      bool
	flush_counts     bool
	pct              out.Percentiles
	flushInterval    int
	max_unprocessed  int
	max_timers_per_s uint64
	debug            bool
	signalchan       chan os.Signal

	Metrics             chan []*common.Metric
	metricAmounts       chan []*common.Metric
	metricStatsRequests chan metricsStatsReq
	valid_lines         *topic.Topic
	Invalid_lines       *topic.Topic
	events              *topic.Topic

	Clock         clock.Clock
	submitFunc    SubmitFunc
	graphiteQueue chan []byte

	listen_addr   string
	admin_addr    string
	graphite_addr string
}

func New(instance string, formatter out.Formatter, flush_rates, flush_counts bool, pct out.Percentiles, flushInterval, max_unprocessed int, max_timers_per_s uint64, signalchan chan os.Signal) *StatsDaemon {
	return &StatsDaemon{
		instance:            instance,
		fmt:                 formatter,
		flush_rates:         flush_rates,
		flush_counts:        flush_counts,
		pct:                 pct,
		flushInterval:       flushInterval,
		max_unprocessed:     max_unprocessed,
		max_timers_per_s:    max_timers_per_s,
		signalchan:          signalchan,
		Metrics:             make(chan []*common.Metric, max_unprocessed),
		metricAmounts:       make(chan []*common.Metric, max_unprocessed),
		metricStatsRequests: make(chan metricsStatsReq),
		valid_lines:         topic.New(),
		Invalid_lines:       topic.New(),
		events:              topic.New(),
	}
}

// start statsdaemon instance with standard network daemon behaviors
func (s *StatsDaemon) Run(listen_addr, admin_addr, graphite_addr string) {
	s.Clock = clock.New()
	s.submitFunc = s.GraphiteQueue
	s.graphiteQueue = make(chan []byte, 1000)

	s.listen_addr = listen_addr
	s.admin_addr = admin_addr
	s.graphite_addr = graphite_addr

	log.Infof("statsdaemon instance '%s' starting\n", s.instance)
	output := &out.Output{
		Metrics:       s.Metrics,
		MetricAmounts: s.metricAmounts,
		Valid_lines:   s.valid_lines,
		Invalid_lines: s.Invalid_lines,
	}
	go udp.StatsListener(s.listen_addr, s.fmt.PrefixInternal, output) // set up udp listener that writes messages to output's channels (i.e. s's channels)
	go s.adminListener()                                              // tcp admin_addr to handle requests
	go s.metricStatsMonitor()                                         // handles requests fired by telnet api
	go s.graphiteWriter()                                             // writes to graphite in the background
	s.metricsMonitor()                                                // takes data from s.Metrics and puts them in the guage/timers/etc objects. pointers guarded by select. also listens for signals.
}

// start statsdaemon instance, only processing incoming metrics from the channel, and flushing
// no admin listener
// up to you to write to Metrics and metricAmounts channels, and set submitFunc, and set the clock

func (s *StatsDaemon) RunBare() {
	log.Infof("statsdaemon instance '%s' starting\n", s.instance)
	go s.metricStatsMonitor()
	s.metricsMonitor()
}

// metricsMonitor basically guards the metrics datastructures.
// it typically receives metrics on the Metrics channel but also responds to
// external signals and every flushInterval, computes and flushes the data
func (s *StatsDaemon) metricsMonitor() {
	period := time.Duration(s.flushInterval) * time.Second
	tick := ticker.GetAlignedTicker(s.Clock, period)

	var c *out.Counters
	var g *out.Gauges
	var t *out.Timers
	oneCounter := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_counter.mtype_is_count.unit_is_Metric", s.fmt.PrefixInternal),
		Value:    1,
		Sampling: 1,
	}
	oneGauge := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_gauge.mtype_is_count.unit_is_Metric", s.fmt.PrefixInternal),
		Value:    1,
		Sampling: 1,
	}
	oneTimer := &common.Metric{
		Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_timer.mtype_is_count.unit_is_Metric", s.fmt.PrefixInternal),
		Value:    1,
		Sampling: 1,
	}

	initializeCounters := func() {
		c = out.NewCounters(s.flush_rates, s.flush_counts)
		g = out.NewGauges()
		t = out.NewTimers(s.pct)
		for _, name := range []string{"timer", "gauge", "counter"} {
			c.Add(&common.Metric{
				Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_%s.mtype_is_count.unit_is_Metric", s.fmt.PrefixInternal, name),
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
			go func(c *out.Counters, g *out.Gauges, t *out.Timers) {
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

// instrument wraps around a processing function, and makes sure we track the number of metrics and duration of the call,
// which it flushes as metrics2.0 metrics to the outgoing buffer.
func (s *StatsDaemon) instrument(st out.Type, buf []byte, now int64, name string) ([]byte, int64) {
	time_start := s.Clock.Now()
	buf, num := st.Process(buf, now, s.flushInterval, s.fmt)
	time_end := s.Clock.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	buf = out.WriteFloat64(buf, []byte(fmt.Sprintf("%s%sstatsd_type_is_%s.mtype_is_gauge.type_is_calculation.unit_is_ms", s.fmt.Prefix_m20ne_gauges, s.fmt.PrefixInternal, name)), duration_ms, now)
	buf = out.WriteFloat64(buf, []byte(fmt.Sprintf("%s%sdirection_is_out.statsd_type_is_%s.mtype_is_rate.unit_is_Metricps", s.fmt.Prefix_m20ne_rates, s.fmt.PrefixInternal, name)), float64(num)/float64(s.flushInterval), now)
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
					log.Infof("now connected to %s", s.graphite_addr)
				} else {
					log.Warnf("dialing %s failed: %s. will retry", s.graphite_addr, err.Error())
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
		if log.IsLevelEnabled(log.DebugLevel) {
			for _, line := range bytes.Split(buf, []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				log.Debugf("writing %s", line)
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
				log.Debug("wrote metrics payload to graphite!")
			} else {
				log.Errorf("failed to write to graphite: %s (took %s). will retry...", err, s.Clock.Now().Sub(pre))
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
		buf = out.WriteFloat64(buf, []byte(fmt.Sprintf("%s%smtype_is_gauge.type_is_send.unit_is_ms", s.fmt.Prefix_m20ne_gauges, s.fmt.PrefixInternal)), duration, pre.Unix())
		ok = false
		for !ok {
			lock.Lock()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
				log.Debug("wrote sendtime to graphite!")
			} else {
				log.Errorf("failed to write mtype_is_gauge.type_is_send.unit_is_ms: %s. will retry...", err)
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
func (s *StatsDaemon) GraphiteQueue(c *out.Counters, g *out.Gauges, t *out.Timers, deadline time.Time) {
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
		log.Debug("[api] received command: '" + clean_cmd + "'")
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
	log.Info("Listening on " + s.admin_addr)
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
