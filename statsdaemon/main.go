package main

import (
	"flag"
	"fmt"
	"github.com/vimeo/statsdaemon/timers"
	"github.com/vimeo/statsdaemon"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/stvp/go-toml-config"
	"net/http"
	_ "net/http/pprof"
)

const (
	VERSION = "0.5.2-alpha"
	// number of packets we can read out of udp buffer without processing them
	// statsdaemon doesn't really interrupt the udp reader like some other statsd's do (like on flush)
	// but this can still be useful to deal with traffic bursts.
	// keep in mind that one metric is about 30 to 100 bytes of memory.
	MAX_UNPROCESSED_PACKETS = 1000000
)

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
	max_timers_per_s      = config.Uint64("max_timers_per_s", 1000)

	debug       = flag.Bool("debug", false, "log outgoing metrics, bad lines, and received admin commands")
	showVersion = flag.Bool("version", false, "print version string")
	config_file = flag.String("config_file", "/etc/statsdaemon.ini", "config file location")
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile  = flag.String("memprofile", "", "write memory profile to this file")
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

	daemon := statsdaemon.New(inst, *listen_addr, *admin_addr, *graphite_addr, *prefix_rates, *prefix_timers, *prefix_gauges, *pct, *flushInterval, MAX_UNPROCESSED_PACKETS, *max_timers_per_s, signalchan, *debug)
	if *debug {
		consumer := make(chan interface{}, 100)
		daemon.Invalid_lines.Register(consumer)
		go func() {
			for line := range consumer {
				log.Printf("invalid line '%s'\n", line)
			}
		}()
	}
	daemon.Run()

}
