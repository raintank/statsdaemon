package statsdaemon

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bmizerany/assert"
	"github.com/vimeo/statsdaemon/common"
	"github.com/vimeo/statsdaemon/counters"
	"github.com/vimeo/statsdaemon/gauges"
	"github.com/vimeo/statsdaemon/timers"
	"github.com/vimeo/statsdaemon/udp"
)

var output = common.NullOutput()
var prefix_internal = ""

func TestPacketParse(t *testing.T) {
	d := []byte("gaugor:333|g")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet := packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(333), packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:2|c|@0.1")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(2), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(-4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.Value)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
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

	errors_key := "target_type_is_count.type_is_invalid_line.unit_is_Err"
	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 2)
	assert.Equal(t, packets[0].Bucket, errors_key)
	assert.Equal(t, packets[1].Bucket, errors_key)

	d = []byte("a.key.with-0.dash:4")
	packets = udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)
	assert.Equal(t, len(packets), 1)
	assert.Equal(t, packets[0].Bucket, errors_key)
}

func TestMean(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("response_time:0|ms\nresponse_time:30|ms\nresponse_time:30|ms")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	ti := timers.New("", timers.Percentiles{})

	for _, p := range packets {
		ti.Add(p)
	}
	var buff bytes.Buffer
	num := ti.Process(&buff, time.Now().Unix(), 60)
	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()
	pattern := `response_time\.mean 20\.[0-9]+ `
	meanRegexp := regexp.MustCompile(pattern)

	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}

func getGraphiteSendForCounter(cnt *counters.Counters, input string) (string, int64) {
	// Some data with expected sum of 6
	d := []byte(input)
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	for _, p := range packets {
		cnt.Add(p)
	}

	var buff bytes.Buffer
	num := cnt.Process(&buff, 1, 10)
	return buff.String(), num
}

func TestCountersLegacyNamespaceFalse(t *testing.T) {
	cnt := counters.New("rates.", "counters.", false, true, true)
	dataForGraphite, num := getGraphiteSendForCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c")

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "counters.logins.count 6.000000 1\nrates.logins.rate 0.600000 1\n", dataForGraphite)
}

func TestCountersLegacyNamespaceTrue(t *testing.T) {
	cnt := counters.New("stats.", "stats_counts.", true, true, true)
	dataForGraphite, num := getGraphiteSendForCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c")

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "stats_counts.logins 6.000000 1\nstats.logins 0.600000 1\n", dataForGraphite)
}

func TestCountersLegacyNamespaceTrueFlushCountsFalse(t *testing.T) {
	cnt := counters.New("stats.", "stats_counts.", true, true, false)
	dataForGraphite, num := getGraphiteSendForCounter(cnt, "logins:1|c\nlogins:2|c\nlogins:3|c")

	assert.Equal(t, num, int64(1))
	assert.Equal(t, "stats.logins 0.600000 1\n", dataForGraphite)
}

func TestUpperPercentile(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	pct, _ := timers.NewPercentiles("75")
	ti := timers.New("", *pct)

	for _, p := range packets {
		ti.Add(p)
	}

	var buff bytes.Buffer
	num := ti.Process(&buff, time.Now().Unix(), 60)
	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()

	meanRegexp := regexp.MustCompile(`time\.upper_75 2\.`)
	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}

func TestMetrics20Timer(t *testing.T) {
	d := []byte("foo=bar.target_type=gauge.unit=ms:5|ms\nfoo=bar.target_type=gauge.unit=ms:10|ms")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	pct, _ := timers.NewPercentiles("75")
	ti := timers.New("", *pct)

	for _, p := range packets {
		ti.Add(p)
	}

	var buff bytes.Buffer
	num := ti.Process(&buff, time.Now().Unix(), 10)
	assert.Equal(t, int(num), 1)

	dataForGraphite := buff.String()
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=max_75 10.000000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=mean_75 7.500000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=sum_75 15.000000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=mean 7.500000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=median 7.500000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=std 2.500000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=sum 15.000000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=max 10.000000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=gauge.unit=ms.stat=min 5.000000"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=count.unit=Pckt.orig_unit=ms.pckt_type=sent.direction=in 2"))
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=rate.unit=Pcktps.orig_unit=ms.pckt_type=sent.direction=in 0.200000"))
}
func TestMetrics20Count(t *testing.T) {
	d := []byte("foo=bar.target_type=count.unit=B:5|c\nfoo=bar.target_type=count.unit=B:10|c")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	c := counters.New("", "", true, true, false)
	for _, p := range packets {
		c.Add(p)
	}

	var buff bytes.Buffer
	var num int64
	num += c.Process(&buff, time.Now().Unix(), 10)

	dataForGraphite := buff.String()
	assert.T(t, strings.Contains(dataForGraphite, "foo=bar.target_type=rate.unit=Bps 1.5"))
}

func TestLowerPercentile(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := udp.ParseMessage(d, prefix_internal, output, udp.ParseLine)

	pct, _ := timers.NewPercentiles("-75")
	ti := timers.New("", *pct)

	for _, p := range packets {
		ti.Add(p)
	}

	var buff bytes.Buffer
	var num int64
	num += ti.Process(&buff, time.Now().Unix(), 10)

	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()

	meanRegexp := regexp.MustCompile(`time\.upper_75 1\.`)
	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, false)

	meanRegexp = regexp.MustCompile(`time\.lower_75 1\.`)
	matched = meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}

func BenchmarkMillionDifferentCountersAddAndProcess(b *testing.B) {
	metrics := getDifferentCounters(1000000)
	b.ResetTimer()
	c := counters.New("bar", "", true, true, false)
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			c.Add(&metrics[i])
		}
	}
	c.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkMillionSameCountersAddAndProcess(b *testing.B) {
	metrics := getSameCounters(1000000)
	b.ResetTimer()
	c := counters.New("bar", "", true, true, true)
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			c.Add(&metrics[i])
		}
	}
	c.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkMillionDifferentGaugesAddAndProcess(b *testing.B) {
	metrics := getDifferentGauges(1000000)
	b.ResetTimer()
	g := gauges.New("bar")
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			g.Add(&metrics[i])
		}
	}
	g.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkMillionSameGaugesAddAndProcess(b *testing.B) {
	metrics := getSameGauges(1000000)
	b.ResetTimer()
	g := gauges.New("bar")
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			g.Add(&metrics[i])
		}
	}
	g.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkMillionDifferentTimersAddAndProcess(b *testing.B) {
	metrics := getDifferentTimers(1000000)
	b.ResetTimer()
	pct, _ := timers.NewPercentiles("99")
	t := timers.New("bar", *pct)
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			t.Add(&metrics[i])
		}
	}
	t.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkMillionSameTimersAddAndProcess(b *testing.B) {
	metrics := getSameTimers(1000000)
	b.ResetTimer()
	pct, _ := timers.NewPercentiles("99")
	t := timers.New("bar", *pct)
	for i := 0; i < len(metrics); i++ {
		for n := 0; n < b.N; n++ {
			t.Add(&metrics[i])
		}
	}
	t.Process(&bytes.Buffer{}, time.Now().Unix(), 10)
}

func BenchmarkIncomingMetrics(b *testing.B) {
	daemon := New("test", "rates.", "timers.", "gauges.", "counters.", timers.Percentiles{}, 10, 1000, 1000, nil, false, true, true, false)
	daemon.Clock = clock.NewMock()
	total := float64(0)
	totalLock := sync.Mutex{}
	daemon.submitFunc = func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time) error {
		totalLock.Lock()
		total += c.Values["service_is_statsdaemon.instance_is_test.direction_is_in.statsd_type_is_counter.target_type_is_count.unit_is_Metric"]
		totalLock.Unlock()
		return nil
	}
	go daemon.RunBare()
	b.ResetTimer()
	counters := make([]*common.Metric, 10)
	for i := 0; i < 10; i++ {
		counters[i] = &common.Metric{
			"test-counter",
			float64(1),
			"c",
			float32(1),
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
			panic(fmt.Sprintf("didn't see 1M counters. only saw %d", total))
		}
		totalLock.Unlock()
	}

}

func BenchmarkIncomingMetricAmounts(b *testing.B) {
	daemon := New("test", "rates.", "timers.", "gauges.", "counters.", timers.Percentiles{}, 10, 1000, 1000, nil, false, true, true, false)
	daemon.Clock = clock.NewMock()
	daemon.submitFunc = func(c *counters.Counters, g *gauges.Gauges, t *timers.Timers, deadline time.Time) error {
		return nil
	}
	go daemon.RunBare()
	b.ResetTimer()
	counters := make([]*common.Metric, 10)
	for i := 0; i < 10; i++ {
		counters[i] = &common.Metric{
			"test-counter",
			float64(1),
			"c",
			float32(1),
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
