package statsdaemon

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bmizerany/assert"
	"github.com/raintank/statsdaemon/common"
	"github.com/raintank/statsdaemon/out"
	"github.com/raintank/statsdaemon/udp"
)

var output = out.NullOutput()

var formatM1Legacy = out.Formatter{
	PrefixInternal:   "internal.",
	Legacy_namespace: true,
	Prefix_rates:     "stats.",
	Prefix_counters:  "stats_counts.",
	Prefix_timers:    "stats.timers.",
	Prefix_gauges:    "stats.gauges.",
}

var formatM1Recommended = out.Formatter{
	PrefixInternal:   "internal.",
	Legacy_namespace: false,
	Prefix_rates:     "stats.counters.",
	Prefix_counters:  "stats.counters.",
	Prefix_timers:    "stats.timers.",
	Prefix_gauges:    "stats.gauges.",
}

var formatM20 = out.Formatter{
	Prefix_m20_counters: "counters-2.",
	Prefix_m20_gauges:   "gauges-2.",
	Prefix_m20_rates:    "rates-2.",
	Prefix_m20_timers:   "timers-2.",
}

var formatM20NE = out.Formatter{
	Prefix_m20ne_counters: "counters-2NE.",
	Prefix_m20ne_gauges:   "gauges-2NE.",
	Prefix_m20ne_rates:    "rates-2NE.",
	Prefix_m20ne_timers:   "timers-2NE.",
}

func TestPacketParse(t *testing.T) {
	d := []byte("gaugor:333|g")
	packets := udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet := packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(333), packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:2|c|@0.1")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(2), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(-4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.Value)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 2)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet = packets[1]
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, float64(3), packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	errors_key := "internal.mtype_is_count.type_is_invalid_line.unit_is_Err"
	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 2)
	assert.Equal(t, packets[0].Bucket, errors_key)
	assert.Equal(t, packets[1].Bucket, errors_key)

	d = []byte("a.key.with-0.dash:4")
	packets = udp.ParseMessage(d, formatM1Legacy.PrefixInternal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	assert.Equal(t, packets[0].Bucket, errors_key)
}

func processTimer(ti *out.Timers, input string, f out.Formatter) (string, int64) {
	packets := udp.ParseMessage([]byte(input), "", output, udp.ParseLine)
	for _, p := range packets {
		ti.Add(p)
	}
	buf, num := ti.Process(nil, time.Now().Unix(), 10, f)
	return string(buf), num
}

func processCounter(cnt *out.Counters, input string, f out.Formatter) (string, int64) {
	packets := udp.ParseMessage([]byte(input), "", output, udp.ParseLine)
	for _, p := range packets {
		cnt.Add(p)
	}

	buf, num := cnt.Process(nil, 1, 10, f)
	return string(buf), num
}

func TestTimerM1(t *testing.T) {
	got, num := processTimer(out.NewTimers(out.Percentiles{}), "response_time:0|ms\nresponse_time:30|ms\nresponse_time:30|ms", formatM1Legacy)
	assert.Equal(t, num, int64(1))
	exp := "stats.timers.response_time.mean 20 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "stats.timers.response_time.sum 60 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "stats.timers.response_time.lower 0 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "stats.timers.response_time.upper 30 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
}

func TestTimerM20(t *testing.T) {
	pct, _ := out.NewPercentiles("75")
	got, num := processTimer(out.NewTimers(*pct), "direction=out.unit=ms.mtype=gauge:0|ms\ndirection=out.unit=ms.mtype=gauge:30|ms\ndirection=out.unit=ms.mtype=gauge:30|ms", formatM20)
	assert.Equal(t, num, int64(1))
	exps := []string{

		"timers-2.direction=out.unit=ms.mtype=gauge.stat=mean 20 ",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=sum 60 ",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=min 0 ",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=max 30 ",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=max_75 30",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=mean_75 15",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=median 30",
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=std 14.142135623730951", // sqrt((20^2 + 10^2 + 10^2)/3)
		"timers-2.direction=out.unit=ms.mtype=gauge.stat=sum 60",
		"timers-2.direction=out.unit=Pckt.mtype=count.orig_unit=ms.pckt_type=sent.direction=in 3",
		"timers-2.direction=out.unit=Pcktps.mtype=rate.orig_unit=ms.pckt_type=sent.direction=in 0.3",
	}
	for _, exp := range exps {
		if !strings.Contains(got, exp) {
			t.Fatalf("output %q does not contain %q", got, exp)
		}
	}

}

func TestTimerM20NE(t *testing.T) {
	got, num := processTimer(out.NewTimers(out.Percentiles{}), "direction_is_out.unit_is_ms.mtype_is_gauge:0|ms\ndirection_is_out.unit_is_ms.mtype_is_gauge:30|ms\ndirection_is_out.unit_is_ms.mtype_is_gauge:30|ms", formatM20NE)
	assert.Equal(t, num, int64(1))
	exp := "timers-2NE.direction_is_out.unit_is_ms.mtype_is_gauge.stat_is_mean 20 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "timers-2NE.direction_is_out.unit_is_ms.mtype_is_gauge.stat_is_sum 60 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "timers-2NE.direction_is_out.unit_is_ms.mtype_is_gauge.stat_is_min 0 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
	exp = "timers-2NE.direction_is_out.unit_is_ms.mtype_is_gauge.stat_is_max 30 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
}

func TestCountersM1Recommended(t *testing.T) {
	cnt := out.NewCounters(true, true)
	dataForGraphite, num := processCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c", formatM1Recommended)

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "stats.counters.logins.count 6 1\nstats.counters.logins.rate 0.6 1\n", dataForGraphite)
}

func TestCountersM1Legacy(t *testing.T) {
	cnt := out.NewCounters(true, true)
	dataForGraphite, num := processCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c", formatM1Legacy)

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "stats_counts.logins 6 1\nstats.logins 0.6 1\n", dataForGraphite)
}

func TestCountersM1LegacyFlushCountsFalse(t *testing.T) {
	cnt := out.NewCounters(true, false)
	dataForGraphite, num := processCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c", formatM1Legacy)

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "stats.logins 0.6 1\n", dataForGraphite)
}

func TestUpperPercentile(t *testing.T) {
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := udp.ParseMessage(d, "", output, udp.ParseLine)

	pct, _ := out.NewPercentiles("75")
	ti := out.NewTimers(*pct)

	for _, p := range packets {
		ti.Add(p)
	}

	var buf []byte
	buf, num := ti.Process(buf, time.Now().Unix(), 60, formatM1Legacy)
	assert.Equal(t, num, int64(1))

	exp := "stats.timers.time.upper_75 2 "
	got := string(buf)
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
}

func TestMetrics20Count(t *testing.T) {
	d := []byte("foo=bar.mtype=count.unit=B:5|c\nfoo=bar.mtype=count.unit=B:10|c")
	packets := udp.ParseMessage(d, "", output, udp.ParseLine)

	c := out.NewCounters(true, false)
	for _, p := range packets {
		c.Add(p)
	}

	var buf []byte
	var num int64
	buf, n := c.Process(buf, time.Now().Unix(), 10, formatM20)
	num += n

	assert.T(t, strings.Contains(string(buf), "foo=bar.mtype=rate.unit=Bps 1.5"))
}

func TestLowerPercentile(t *testing.T) {
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := udp.ParseMessage(d, "", output, udp.ParseLine)

	pct, _ := out.NewPercentiles("-75")
	ti := out.NewTimers(*pct)

	for _, p := range packets {
		ti.Add(p)
	}

	var buf []byte
	var num int64
	buf, n := ti.Process(buf, time.Now().Unix(), 10, formatM1Legacy)
	num += n

	assert.Equal(t, num, int64(1))

	exp := "time.upper_75 1 "
	got := string(buf)
	if strings.Contains(got, exp) {
		t.Fatalf("output %q contains %q", got, exp)
	}

	exp = "time.lower_75 1 "
	if !strings.Contains(got, exp) {
		t.Fatalf("output %q does not contain %q", got, exp)
	}
}

func BenchmarkDifferentCountersAddAndProcessM1Recommended(b *testing.B) {
	metrics := getDifferentCounters(b.N)
	b.ResetTimer()
	c := out.NewCounters(true, false)
	for i := 0; i < len(metrics); i++ {
		c.Add(&metrics[i])
	}
	c.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Recommended)
}

func BenchmarkDifferentCountersAddAndProcessM1Legacy(b *testing.B) {
	metrics := getDifferentCounters(b.N)
	b.ResetTimer()
	c := out.NewCounters(true, true)
	for i := 0; i < len(metrics); i++ {
		c.Add(&metrics[i])
	}
	c.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkSameCountersAddAndProcessM1Recommended(b *testing.B) {
	metrics := getSameCounters(b.N)
	b.ResetTimer()
	c := out.NewCounters(true, false)
	for i := 0; i < len(metrics); i++ {
		c.Add(&metrics[i])
	}
	c.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Recommended)
}

func BenchmarkSameCountersAddAndProcessM1Legacy(b *testing.B) {
	metrics := getSameCounters(b.N)
	b.ResetTimer()
	c := out.NewCounters(true, true)
	for i := 0; i < len(metrics); i++ {
		c.Add(&metrics[i])
	}
	c.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkDifferentGaugesAddAndProcess(b *testing.B) {
	metrics := getDifferentGauges(b.N)
	b.ResetTimer()
	g := out.NewGauges()
	for i := 0; i < len(metrics); i++ {
		g.Add(&metrics[i])
	}
	g.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkSameGaugesAddAndProcess(b *testing.B) {
	metrics := getSameGauges(b.N)
	b.ResetTimer()
	g := out.NewGauges()
	for i := 0; i < len(metrics); i++ {
		g.Add(&metrics[i])
	}
	g.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkDifferentTimersAddAndProcess(b *testing.B) {
	metrics := getDifferentTimers(b.N)
	b.ResetTimer()
	pct, _ := out.NewPercentiles("99")
	t := out.NewTimers(*pct)
	for i := 0; i < len(metrics); i++ {
		t.Add(&metrics[i])
	}
	t.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkSameTimersAddAndProcess(b *testing.B) {
	metrics := getSameTimers(b.N)
	b.ResetTimer()
	pct, _ := out.NewPercentiles("99")
	t := out.NewTimers(*pct)
	for i := 0; i < len(metrics); i++ {
		t.Add(&metrics[i])
	}
	t.Process(make([]byte, 0), time.Now().Unix(), 10, formatM1Legacy)
}

func BenchmarkIncomingMetrics(b *testing.B) {
	daemon := New("test", formatM1Legacy, false, false, out.Percentiles{}, 10, 1000, 1000, nil)
	daemon.Clock = clock.NewMock()
	total := float64(0)
	totalLock := sync.Mutex{}
	daemon.submitFunc = func(c *out.Counters, g *out.Gauges, t *out.Timers, deadline time.Time) {
		totalLock.Lock()
		total += c.Values["internal.direction_is_in.statsd_type_is_counter.mtype_is_count.unit_is_Metric"]
		totalLock.Unlock()
	}
	go daemon.RunBare()
	b.ResetTimer()
	counters := make([]*common.Metric, 10)
	for i := 0; i < 10; i++ {
		counters[i] = &common.Metric{
			Bucket:   "test-counter",
			Value:    float64(1),
			Modifier: "c",
			Sampling: float32(1),
		}
	}
	// each operation consists of 100x write (1k * 10 metrics + move clock by 1second)
	// simulating a fake 10k metrics/s load, 1M metrics in total over 100+10s, so 11 flushes
	for n := 0; n < b.N; n++ {
		totalLock.Lock()
		total = 0
		totalLock.Unlock()
		for j := 0; j < 100; j++ {
			for i := 0; i < 1000; i++ {
				daemon.Metrics <- counters
			}
			daemon.Clock.(*clock.Mock).Add(1 * time.Second)
		}
		daemon.Clock.(*clock.Mock).Add(10 * time.Second)
		totalLock.Lock()
		if total != float64(1000000) {
			panic(fmt.Sprintf("didn't see 1M counters. only saw %f", total))
		}
		totalLock.Unlock()
	}

}

func BenchmarkIncomingMetricAmounts(b *testing.B) {
	daemon := New("test", formatM1Legacy, false, false, out.Percentiles{}, 10, 1000, 1000, nil)
	daemon.Clock = clock.NewMock()
	daemon.submitFunc = func(c *out.Counters, g *out.Gauges, t *out.Timers, deadline time.Time) {
	}
	go daemon.RunBare()
	b.ResetTimer()
	counters := make([]*common.Metric, 10)
	for i := 0; i < 10; i++ {
		counters[i] = &common.Metric{
			Bucket:   "test-counter",
			Value:    float64(1),
			Modifier: "c",
			Sampling: float32(1),
		}
	}
	// each operation consists of 100x write (1k * 10 metrics + move clock by 1second)
	// simulating a fake 10k metrics/s load, 1M metrics in total over 100+10s, so 11 flushes
	for n := 0; n < b.N; n++ {
		for j := 0; j < 100; j++ {
			for i := 0; i < 1000; i++ {
				daemon.metricAmounts <- counters
			}
			daemon.Clock.(*clock.Mock).Add(1 * time.Second)
		}
		daemon.Clock.(*clock.Mock).Add(10 * time.Second)
	}

}
