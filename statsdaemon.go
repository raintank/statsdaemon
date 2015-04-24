package statsdaemon

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/tv42/topic"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/counters"
	"github.com/vimeo/statsdaemon/gauges"
	"github.com/vimeo/statsdaemon/timers"
	"github.com/vimeo/statsdaemon/udp"
	"github.com/vimeo/statsdaemon/ticker"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"syscall"
	"time"
)

type metricsStatsReq struct {
	Command []string
	Conn    *net.Conn
}

type StatsDaemon struct {
	instance            string
	listen_addr         string
	admin_addr          string
	graphite_addr       string
	prefix              string
	prefix_rates        string
	prefix_timers       string
	prefix_gauges       string
	pct                 timers.Percentiles
	flushInterval       int
    max_unprocessed int
    max_timers_per_s    uint64
	signalchan          chan os.Signal
	Metrics             chan *common.Metric
	metricAmounts       chan common.MetricAmount
	metricStatsRequests chan metricsStatsReq
	valid_lines         *topic.Topic
	Invalid_lines       *topic.Topic
	events              *topic.Topic
	debug               bool
}

func New(instance, listen_addr, admin_addr, graphite_addr, prefix_rates, prefix_timers, prefix_gauges string, pct timers.Percentiles, flushInterval, max_unprocessed int, max_timers_per_s uint64, signalchan chan os.Signal, debug bool) *StatsDaemon {
	return &StatsDaemon{
		instance,
		listen_addr,
		admin_addr,
		graphite_addr,
		"service_is_statsdaemon.instance_is_" + instance + ".",
		prefix_rates,
		prefix_timers,
		prefix_gauges,
		pct,
		flushInterval,
        max_unprocessed,
        max_timers_per_s,
		signalchan,
		make(chan *common.Metric, max_unprocessed),
		make(chan common.MetricAmount, max_unprocessed),
		make(chan metricsStatsReq),
		topic.New(),
		topic.New(),
		topic.New(),
		debug,
	}
}

func (s *StatsDaemon) Run() {
	log.Printf("statsdaemon instance '%s' starting\n", s.instance)
	output := &common.Output{s.Metrics, s.metricAmounts, s.valid_lines, s.Invalid_lines}
	go udp.StatsListener(s.listen_addr, s.prefix, output)
	go s.adminListener()
	go s.metricStatsMonitor()
	s.metricsMonitor()
}

// metricsMonitor basically guards the metrics datastructures.
// it typically receives metrics on the Metrics channel but also responds to
// external signals and every flushInterval, computes and flushes the data
func (s *StatsDaemon) metricsMonitor() {
	period := time.Duration(s.flushInterval) * time.Second
	tick := ticker.GetAlignedTicker(period)

	var c *counters.Counters
	var g *gauges.Gauges
	var t *timers.Timers

	initializeCounters := func() {
		c = counters.New(s.prefix_rates)
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
				if err := s.submit(c, g, t, time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				return
			default:
				fmt.Printf("unknown signal %s, ignoring\n", sig)
			}
		case <-tick.C:
			go func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers) {
				if err := s.submit(c, g, t, time.Now().Add(period)); err != nil {
					log.Printf("ERROR: %s", err)
				}
				s.events.Broadcast <- "flush"
			}(c, g, t)
			initializeCounters()
			tick = ticker.GetAlignedTicker(period)
		case m := <-s.Metrics:
			var name string
			if m.Modifier == "ms" {
				t.Add(m)
				name = "timer"
			} else if m.Modifier == "g" {
				g.Add(m)
				name = "gauge"
			} else {
				c.Add(m)
				name = "counter"
			}
			c.Add(&common.Metric{
				Bucket:   fmt.Sprintf("%sdirection_is_in.statsd_type_is_%s.target_type_is_count.unit_is_Metric", s.prefix, name),
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
func (s *StatsDaemon) instrument(st statsdType, buffer *bytes.Buffer, now int64, name string) (num int64) {
	time_start := time.Now()
	num = st.Process(buffer, now, s.flushInterval)
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000000)
	fmt.Fprintf(buffer, "%sstatsd_type_is_%s.target_type_is_gauge.type_is_calculation.unit_is_ms %f %d\n", s.prefix, name, duration_ms, now)
	fmt.Fprintf(buffer, "%sdirection_is_out.statsd_type_is_%s.target_type_is_rate.unit_is_Metricps %f %d\n", s.prefix, name, float64(num)/float64(s.flushInterval), now)
	return
}

// submit basically invokes the processing function (instrumented) and tries to buffer to graphite
func (s *StatsDaemon) submit(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time) error {
	var buffer bytes.Buffer

	now := time.Now().Unix()

	// TODO: in future, buffer up data (with a TTL/max size) and submit later
	client, err := net.Dial("tcp", s.graphite_addr)
	if err != nil {
		c.Process(&buffer, now, s.flushInterval)
		g.Process(&buffer, now, s.flushInterval)
		t.Process(&buffer, now, s.flushInterval)
		errmsg := fmt.Sprintf("dialing %s failed - %s", s.graphite_addr, err.Error())
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		errmsg := fmt.Sprintf("could not set deadline - %s", err.Error())
		return errors.New(errmsg)
	}
	s.instrument(c, &buffer, now, "counter")
	s.instrument(g, &buffer, now, "gauge")
	s.instrument(t, &buffer, now, "timer")

	if s.debug {
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
	if s.debug {
		log.Println("submit() successfully finished")
	}

	buffer.Reset()
	fmt.Fprintf(&buffer, "%starget_type_is_gauge.type_is_send.unit_is_ms %f %d\n", s.prefix, duration_ms, now)
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
func (s *StatsDaemon) metricStatsMonitor() {
	period := 10 * time.Second
	tick := time.NewTicker(period)
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
			swap_ts = time.Now()
		case s_a := <-s.metricAmounts:
			el, ok := (*cur_counts)[s_a.Bucket]
			if ok {
				el.Seen += 1
				el.Submitted += uint64(1 / s_a.Sampling)
			} else {
				(*cur_counts)[s_a.Bucket] = Amounts{uint64(1 / s_a.Sampling), 1}
			}
		case req := <-s.metricStatsRequests:
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
				// submitted (at source) per second * ideal_sample_rate should be ~= max_timers_per_s
				ideal_sample_rate := float64(1)
				if uint64(submitted_per_s) > s.max_timers_per_s {
					ideal_sample_rate = float64(s.max_timers_per_s) / submitted_per_s
				}
				fmt.Fprintf(&resp, "%s %f %f\n", bucket, ideal_sample_rate, submitted_per_s)
				// this needs to be less realtime, so for simplicity (and performance?) we just use the prev 10s bucket.
			case "metric_stats":
				for bucket, el := range *prev_counts {
					fmt.Fprintf(&resp, "%s %f %f\n", bucket, float64(el.Submitted)/10, float64(el.Seen)/10)
				}
			}

			go s.handleApiRequest(*req.Conn, resp)
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
func (s *StatsDaemon) handleApiRequest(conn net.Conn, write_first bytes.Buffer) {
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
		if s.debug {
			log.Println("[api] received command: '" + clean_cmd + "'")
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
		go s.handleApiRequest(conn, bytes.Buffer{})
	}
}
