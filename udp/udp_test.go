package udp

import (
	"errors"
	"github.com/vimeo/statsdaemon/common"
	"reflect"
	"testing"
)

func runTest(t *testing.T, f func([]byte) (*common.Metric, error)) {
	// input format: key:value|modifier[|@samplerate]
	type Case struct {
		Type      string
		In        string
		OutMetric *common.Metric
		OutErr    []error // we allow more than one phrasing
	}

	tests := []Case{
		Case{
			"gauge-int-simple",
			"search.solr.clips.results:78186|g",
			&common.Metric{
				Bucket:   "search.solr.clips.results",
				Value:    78186,
				Modifier: "g",
				Sampling: float32(1),
			},
			nil,
		},
		/*
			Case{
				"counter-int-simple-trailing-white",
				"cliapp1.queue.consumer.VideoFile_PruneSourceFilesV6.processing.10_90_128_162.removed:1|c  ",
				&common.Metric{
					Bucket:   "cliapp1.queue.consumer.VideoFile_PruneSourceFilesV6.processing.10_90_128_162.removed",
					Value:    1,
					Modifier: "c",
					Sampling: float32(1),
				},
				nil,
			},
			Case{
				"  timing-float-with-samplerate-prefix-whitespace",
				"lvimdfs3.object-replicator.partition.update.timing:3.69596481323|ms|@0.05",
				&common.Metric{
					Bucket:   "lvimdfs3.object-replicator.partition.update.timing",
					Value:    3.69596481323,
					Modifier: "ms",
					Sampling: float32(0.05),
				},
				nil,
			},
		*/
		Case{
			"funky-chars",
			"foo%bar=yes:12|ms|@0.05",
			&common.Metric{
				Bucket:   "foo%bar=yes",
				Value:    12,
				Modifier: "ms",
				Sampling: float32(0.05),
			},
			nil,
		},
		Case{
			"middle-whitespace",
			"foo bar:12|ms|@0.05",
			&common.Metric{
				Bucket:   "foo bar",
				Value:    12,
				Modifier: "ms",
				Sampling: float32(0.05),
			},
			nil,
		},
		Case{
			"empty-key",
			":12|ms|@0.05",
			nil,
			[]error{errors.New("key zero len"), errors.New("")},
		},
		Case{
			"empty-val",
			"key:|ms|@0.05",
			nil,
			[]error{errors.New("strconv.ParseFloat: parsing \"\": invalid syntax")},
		},
		Case{
			"empty-mod",
			"key:12||@0.05",
			nil,
			[]error{errors.New("unsupported metric type"), errors.New("invalid modifier")},
		},
		Case{
			"empty-samplerate",
			"key:12|g|@",
			nil,
			[]error{errors.New("strconv.ParseFloat: parsing \"\": invalid syntax")},
		},
		Case{
			"bad-samplerate",
			"key:12|g|@f",
			nil,
			[]error{errors.New("strconv.ParseFloat: parsing \"f\": invalid syntax")},
		},
		Case{
			"missing-modifier",
			"foo_bar:12|@0.05",
			nil,
			[]error{errors.New("unsupported metric type"), errors.New("invalid modifier")},
		},
		Case{
			"invalid-samplerate",
			"foo_bar:12@0.05",
			nil,
			[]error{errors.New("bad amount of pipes"), errors.New("missing value separator")},
		},
	}

	for _, c := range tests {
		metric, err := f([]byte(c.In))
		if c.OutErr == nil {
			if err != nil {
				t.Errorf("case %s failed\nin: %s\nexpected err: %v\nreceived err: %v\n", c.Type, c.In, c.OutErr, err)
			}
		} else {
			good := false
			for _, acceptableErr := range c.OutErr {
				if err != nil && err.Error() == acceptableErr.Error() {
					good = true
					break
				}
			}
			if !good {
				t.Errorf("case %s failed\nin: %s\nexpected one of err: %v\nreceived err:         %v\n", c.Type, c.In, c.OutErr, err)
			}
		}
		metricEqual := reflect.DeepEqual(c.OutMetric, metric)
		if !metricEqual {
			t.Errorf("case %s failed\nin: %s\nexpected metric: %v\nreceived metric: %v\n", c.Type, c.In, c.OutMetric, metric)
		}
	}
}

func runBench(b *testing.B, f func([]byte) (*common.Metric, error)) {
	var err error
	line1 := []byte("cat:12.0231|ms")
	line2 := []byte("meow:45.0231|g|@54.12")
	for i := 0; i < b.N; i++ {
		_, err = f(line1)
		if err != nil {
			b.Fatal(err)
		}
		_, err = f(line2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestParseLine(t *testing.T) {
	runTest(t, ParseLine)
}

func TestParseLine2(t *testing.T) {
	runTest(t, ParseLine2)
}

func BenchmarkParseLine(b *testing.B) {
	runBench(b, ParseLine)
}

func BenchmarkParseLine2(b *testing.B) {
	runBench(b, ParseLine2)
}
