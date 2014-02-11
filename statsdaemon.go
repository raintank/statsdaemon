package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
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

type Packet struct {
	Bucket   string
	Value    float64
	Modifier string
	Sampling float32
}

type Float64Slice []float64

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

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
	graphite_addr        = config.String("graphite_addr", "127.0.0.1:2003")
	flushInterval        = config.Int("flush_interval", 10)
	prefix_rates         = config.String("prefix_rates", "stats.")
	prefix_timers        = config.String("prefix_timers", "stats.timers.")
	prefix_gauges        = config.String("prefix_gauges", "stats.gauges.")
	percentile_tresholds = config.String("percentile_tresholds", "")
	percentThreshold     = Percentiles{}

	debug       = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion = flag.Bool("version", false, "print version string")
	config_file = flag.String("config_file", "/etc/statsdaemon.ini", "config file location")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
)

var (
	In       = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters = make(map[string]float64)
	gauges   = make(map[string]float64)
	timers   = make(map[string]Float64Slice)
)

func monitor() {
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
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t Float64Slice
					timers[s.Bucket] = t
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value)
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = s.Value
			} else {
				v, ok := counters[s.Bucket]
				if !ok || v < 0 {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += s.Value * float64(1/s.Sampling)
			}
		}
	}
}

type processFn func(*bytes.Buffer, int64, Percentiles) int64

func instrument(fun processFn, buffer *bytes.Buffer, now int64, pctls Percentiles, name string) (num int64) {
	time_start := time.Now()
	num = fun(buffer, now, pctls)
	time_end := time.Now()
	duration_ms := float64(time_end.Sub(time_start).Nanoseconds()) / float64(1000)
	log.Printf("stats.statsdaemon.%s.type=%s.what=calculation.unit=ms %f %d\n", "dfvimeographite3", name, duration_ms, now)
	log.Printf("stats.statsdaemon.%s.%s.type=%s.direction=out.unit=metrics %d %d\n", "dfvimeographite3", *graphite_addr, name, num, now)
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
	num += instrument(processCounters, &buffer, now, percentThreshold, "counters")
	num += instrument(processGauges, &buffer, now, percentThreshold, "gauges")
	num += instrument(processTimers, &buffer, now, percentThreshold, "timers")
	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
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
		counters[s] = -1
		v := c / float64(*flushInterval)
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
	// count  estimate of original amount, by dividing received by samplerate
	// count_ps  same, per second
	// lower
	// mean  // arithmetic mean
	// mean_<pct> // arithmetic mean of values below <pct> percentile
	// median
	// std  standard deviation
	// sum
	// sum_90
	// upper
	// upper_90 / lower_90

	// internal vars:
	// seen -> len(t), i.e. real values seen.

	var num int64
	for u, t := range timers {
		if len(t) > 0 {
			seen := len(t)
			num++

			sort.Sort(t)
			min := t[0]
			max := t[len(t)-1]
			count := len(t)
			count_ps := float64(count) / float64(*flushInterval)

			sum := float64(0)
			for _, value := range t {
				sum += value
			}
			mean := float64(sum) / float64(len(t))
			sumOfDiffs := float64(0)
			for _, value := range t {
				sumOfDiffs += math.Pow((float64(value) - mean), 2)
			}
			stddev := math.Sqrt(sumOfDiffs / float64(seen))
			mid := seen / 2
			var median float64
			if seen%2 == 1 {
				median = t[mid]
			} else {
				median = (t[mid-1] + t[mid]) / 2
			}
			var cumulativeValues Float64Slice
			cumulativeValues = make(Float64Slice, seen, seen)
			cumulativeValues[0] = t[0]
			for i := 1; i < seen; i++ {
				cumulativeValues[i] = t[i] + cumulativeValues[i-1]
			}

			maxAtThreshold := max
			sum_pct := sum
			mean_pct := mean

			for _, pct := range pctls {

				if len(t) > 1 {
					var abs float64
					if pct.float >= 0 {
						abs = pct.float
					} else {
						abs = 100 + pct.float
					}
					// poor man's math.Round(x):
					// math.Floor(x + 0.5)
					indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
					if pct.float >= 0 {
						sum_pct = cumulativeValues[indexOfPerc-1]
						maxAtThreshold = t[indexOfPerc-1]
					} else {
						maxAtThreshold = t[indexOfPerc]
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

			var z Float64Slice
			timers[u] = z

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

func parseMessage(data []byte) []*Packet {
	var output []*Packet
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte(":"), 2)
		if len(parts) != 2 {
			if *debug {
				log.Printf("invalid line '%s'\n", line)
			}
			continue
		}
		if bytes.Contains(parts[1], []byte(":")) {
			if *debug {
				log.Printf("invalid line '%s'\n", line)
			}
			continue
		}
		bucket := parts[0]
		parts = bytes.SplitN(parts[1], []byte("|"), 3)
		if len(parts) < 2 {
			if *debug {
				log.Printf("invalid line '%s'\n", line)
			}
			continue
		}
		modifier := string(parts[1])
		if modifier != "g" && modifier != "c" && modifier != "ms" {
			if *debug {
				log.Printf("invalid line '%s'\n", line)
			}
			continue
		}
		sampleRate := float64(1)
		if len(parts) == 3 {
			if parts[2][0] != byte('@') {
				if *debug {
					log.Printf("invalid line '%s'\n", line)
				}
				continue
			}
			var err error
			sampleRate, err = strconv.ParseFloat(string(parts[2])[1:], 32)
			if err != nil {
				if *debug {
					log.Printf("invalid line '%s'\n", line)
				}
				continue
			}
		}
		value, err := strconv.ParseFloat(string(parts[0]), 64)
		if err != nil {
			log.Printf("ERROR: failed to parseFloat %s - %s", parts[0], err)
			continue
		}
		packet := &Packet{
			Bucket:   string(bucket),
			Value:    value,
			Modifier: modifier,
			Sampling: float32(sampleRate),
		}
		output = append(output, packet)
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
			In <- p
		}
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
	config.Parse(*config_file)
	pcts := strings.Split(*percentile_tresholds, ",")
	for _, pct := range pcts {
		percentThreshold.Set(pct)
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan)

	go udpListener()
	monitor()
}
