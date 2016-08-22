package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/Dieterbe/profiletrigger/cpu"
	"github.com/Dieterbe/profiletrigger/heap"
	"github.com/raintank/dur"
	"github.com/vimeo/statsdaemon"
	"github.com/vimeo/statsdaemon/timers"

	"net/http"
	_ "net/http/pprof"

	"github.com/stvp/go-toml-config"
)

const (
	VERSION = "0.6"
	// number of packets we can read out of udp buffer without processing them
	// statsdaemon doesn't really interrupt the udp reader like some other statsd's do (like on flush)
	// but this can still be useful to deal with traffic bursts.
	// keep in mind that one metric is about 30 to 100 bytes of memory.
	MAX_UNPROCESSED_PACKETS = 1000
)

var (
	listen_addr     = config.String("listen_addr", ":8125")
	admin_addr      = config.String("admin_addr", ":8126")
	profile_addr    = config.String("profile_addr", "")
	graphite_addr   = config.String("graphite_addr", "127.0.0.1:2003")
	flushInterval   = config.Int("flush_interval", 10)
	processes       = config.Int("processes", 1)
	instance        = config.String("instance", "null")
	prefix_rates    = config.String("prefix_rates", "stats.")
	prefix_counters = config.String("prefix_counters", "stats_counts.")
	prefix_timers   = config.String("prefix_timers", "stats.timers.")
	prefix_gauges   = config.String("prefix_gauges", "stats.gauges.")

	legacy_namespace = config.Bool("legacy_namespace", true)
	flush_rates      = config.Bool("flush_rates", true)
	flush_counts     = config.Bool("flush_counts", false)

	percentile_thresholds = config.String("percentile_thresholds", "")
	max_timers_per_s      = config.Uint64("max_timers_per_s", 1000)

	proftrigPath = config.String("proftrigger_path", "/tmp") // "path to store triggered profiles"

	proftrigHeapFreqStr    = config.String("proftrigger_heap_freq", "60s")    // "inspect status frequency. set to 0 to disable"
	proftrigHeapMinDiffStr = config.String("proftrigger_heap_min_diff", "1h") // "minimum time between triggered profiles"
	proftrigHeapThresh     = config.Int("proftrigger_heap_thresh", 10000000)  // "if this many bytes allocated, trigger a profile"

	proftrigCpuFreqStr    = config.String("proftrigger_cpu_freq", "60s")    // "inspect status frequency. set to 0 to disable"
	proftrigCpuMinDiffStr = config.String("proftrigger_cpu_min_diff", "1h") // "minimum time between triggered profiles"
	proftrigCpuDurStr     = config.String("proftrigger_cpu_dur", "5s")      // "duration of cpu profile"
	proftrigCpuThresh     = config.Int("proftrigger_cpu_thresh", 80)        // "if this much percent cpu used, trigger a profile"

	debug       = flag.Bool("debug", false, "log outgoing metrics, bad lines, and received admin commands")
	showVersion = flag.Bool("version", false, "print version string")
	config_file = flag.String("config_file", "/etc/statsdaemon.ini", "config file location")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile  = flag.String("memprofile", "", "write memory profile to this file")
	GitHash     = "(none)"
)

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
		fmt.Printf("statsdaemon v%s (built w/%s, git hash %s)\n", VERSION, runtime.Version(), GitHash)
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

	err := config.Parse(*config_file)
	if err != nil {
		fmt.Println(fmt.Sprintf("Could not read config file: %v", err))
		return
	}

	proftrigHeapFreq := dur.MustParseUsec("proftrigger_heap_freq", *proftrigHeapFreqStr)
	proftrigHeapMinDiff := int(dur.MustParseUNsec("proftrigger_heap_min_diff", *proftrigHeapMinDiffStr))

	proftrigCpuFreq := dur.MustParseUsec("proftrigger_cpu_freq", *proftrigCpuFreqStr)
	proftrigCpuMinDiff := int(dur.MustParseUNsec("proftrigger_cpu_min_diff", *proftrigCpuMinDiffStr))
	proftrigCpuDur := int(dur.MustParseUNsec("proftrigger_cpu_dur", *proftrigCpuDurStr))

	if proftrigHeapFreq > 0 {
		errors := make(chan error)
		trigger, _ := heap.New(*proftrigPath, *proftrigHeapThresh, proftrigHeapMinDiff, time.Duration(proftrigHeapFreq)*time.Second, errors)
		go func() {
			for e := range errors {
				log.Printf("profiletrigger heap: %s", e)
			}
		}()
		go trigger.Run()
	}

	if proftrigCpuFreq > 0 {
		errors := make(chan error)
		freq := time.Duration(proftrigCpuFreq) * time.Second
		duration := time.Duration(proftrigCpuDur) * time.Second
		trigger, _ := cpu.New(*proftrigPath, *proftrigCpuThresh, proftrigCpuMinDiff, freq, duration, errors)
		go func() {
			for e := range errors {
				log.Printf("profiletrigger cpu: %s", e)
			}
		}()
		go trigger.Run()
	}

	runtime.GOMAXPROCS(*processes)
	pct, err := timers.NewPercentiles(*percentile_thresholds)
	if err != nil {
		log.Fatal(err)
	}
	inst := os.Expand(*instance, expand_cfg_vars)
	if inst == "" {
		inst = "null"
	}

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan)
	if *profile_addr != "" {
		go func() {
			fmt.Println("Profiling endpoint listening on " + *profile_addr)
			log.Println(http.ListenAndServe(*profile_addr, nil))
		}()
	}

	daemon := statsdaemon.New(inst, *prefix_rates, *prefix_timers, *prefix_gauges, *prefix_counters, *pct, *flushInterval, MAX_UNPROCESSED_PACKETS, *max_timers_per_s, signalchan, *debug, *legacy_namespace, *flush_rates, *flush_counts)
	if *debug {
		consumer := make(chan interface{}, 100)
		daemon.Invalid_lines.Register(consumer)
		go func() {
			for line := range consumer {
				log.Printf("invalid line '%s'\n", line)
			}
		}()
	}
	daemon.Run(*listen_addr, *admin_addr, *graphite_addr)
}
