package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/alexcesaro/statsd"
	"github.com/vimeo/statsdaemon"
	"github.com/vimeo/statsdaemon/timers"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}

	laddr, err := net.ResolveTCPAddr("tcp", "localhost:2003")
	if nil != err {
		panic(err)
	}
	w := NewWatcher(laddr)
	go w.Run()
	pct := timers.Percentiles{}
	daemon := statsdaemon.New("test", ":8125", ":8126", ":2003", "rates.", "timers.", "gauges.", pct, 10, 1000, 1000, nil, false)
	go daemon.Run()
	//daemon must be running otherwise client complains
	time.Sleep(200 * time.Millisecond)
	cl, err := statsd.New(":8125", statsd.WithPrefix("statsd-tester"), statsd.WithFlushPeriod(10*time.Millisecond))
	if nil != err {
		panic(err)
	}

	go func() {
		tick := time.Tick(time.Duration(10) * time.Millisecond)
		for pre := range tick {
			for i := 0; i < 10000; i++ {
				cl.Count("test-counter", 1, 1)

			}
			dur := time.Now().Sub(pre)
			if dur > time.Duration(5)*time.Millisecond {
				fmt.Println("sender took %s : possible sender iteration skipped", dur)
			}
		}
	}()
	time.Sleep(10 * 11 * time.Second)
	panic("timeout: watcher did not get all updates from statsdaemon")

}

type watcher struct {
	l      *net.TCPListener
	seen   int
	values chan string
}

func NewWatcher(laddr *net.TCPAddr) *watcher {
	l, err := net.ListenTCP("tcp", laddr)
	if nil != err {
		panic(err)
	}
	return &watcher{
		l,
		0,
		make(chan string),
	}
}

func (w *watcher) Run() {
	go w.accept()
	counter_per_s_key := "service_is_statsdaemon.instance_is_test.direction_is_in.statsd_type_is_counter.target_type_is_rate.unit_is_Metricps"
	for {
		select {
		case str := <-w.values:
			if strings.HasPrefix(str, counter_per_s_key) {
				vals := strings.Fields(str)
				fmt.Println("counters received per second:", vals[1])
				w.seen += 1
				if w.seen == 10 {
					os.Exit(0)
				}
			}
		}
	}
}

func (w *watcher) accept() {
	for {
		c, err := w.l.AcceptTCP()
		if nil != err {
			panic(err)
		}
		go w.handle(c)
	}
}
func (w *watcher) handle(c *net.TCPConn) {
	defer c.Close()
	r := bufio.NewReaderSize(c, 4096)
	for {
		buf, _, err := r.ReadLine()
		if nil != err {
			if io.EOF != err {
				panic(err)
			}
			break
		}
		str := string(buf)
		w.values <- str
	}
}
