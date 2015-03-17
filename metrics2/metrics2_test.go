package metrics2

import (
	"github.com/bmizerany/assert"
	"strings"
	"testing"
)

type Case struct {
	in  string
	out string
}

func TestDerive_Count(t *testing.T) {
	// undefined edge case:
	// assert.Equal(t, Derive_Count("foo.bar.aunit=no.baz", "prefix."), "prefix.foo.bar.aunit=no.baz")
	// assert.Equal(t, Derive_Count("foo.bar.UNIT=no.baz", "prefix."), "prefix.foo.bar.UNIT=no.baz")

	// not metrics2.0, use prefix
	assert.Equal(t, Derive_Count("foo.bar.unita=no.bar", "prefix."), "prefix.foo.bar.unita=no.bar")
	assert.Equal(t, Derive_Count("foo.bar.target_type=count.baz", "prefix."), "prefix.foo.bar.target_type=count.baz")
	assert.Equal(t, Derive_Count("foo.bar.target_type=count", "prefix."), "prefix.foo.bar.target_type=count")
	assert.Equal(t, Derive_Count("target_type=count.foo.bar", "prefix."), "prefix.target_type=count.foo.bar")

	// metrics 2.0 cases with equals
	cases := []Case{
		Case{"foo.bar.unit=yes.baz", "foo.bar.unit=yesps.baz"},
		Case{"foo.bar.unit=yes", "foo.bar.unit=yesps"},
		Case{"unit=yes.foo.bar", "unit=yesps.foo.bar"},
		Case{"target_type=count.foo.unit=ok.bar", "target_type=rate.foo.unit=okps.bar"},
	}
	for _, c := range cases {
		assert.Equal(t, Derive_Count(c.in, "prefix."), c.out)
	}

	// same but with equals
	for i, c := range cases {
		cases[i] = Case{
			strings.Replace(c.in, "=", "_is_", -1),
			strings.Replace(c.out, "=", "_is_", -1),
		}
	}
	for _, c := range cases {
		assert.Equal(t, Derive_Count(c.in, "prefix."), c.out)
	}
}

// only 1 kind of stat is enough, cause they all behave the same
func TestStat(t *testing.T) {
	cases := []Case{
		Case{"foo.bar.unit=yes.baz", "foo.bar.unit=yes.baz.stat=upper_90"},
		Case{"foo.bar.unit=yes", "foo.bar.unit=yes.stat=upper_90"},
		Case{"unit=yes.foo.bar", "unit=yes.foo.bar.stat=upper_90"},
		Case{"target_type=count.foo.unit=ok.bar", "target_type=count.foo.unit=ok.bar.stat=upper_90"},
	}
	for _, c := range cases {
		assert.Equal(t, Upper(c.in, "prefix.", "90"), c.out)
	}
	// same but with equals and no percentile
	for i, c := range cases {
		cases[i] = Case{
			strings.Replace(c.in, "=", "_is_", -1),
			strings.Replace(strings.Replace(c.out, "=", "_is_", -1), "upper_90", "upper", 1),
		}
	}
	for _, c := range cases {
		assert.Equal(t, Upper(c.in, "prefix.", ""), c.out)
	}
}
func TestRateCountPckt(t *testing.T) {
	cases := []Case{
		Case{"foo.bar.unit=yes.baz", "foo.bar.unit=Pckt.baz.orig_unit=yes.pckt_type=sent.direction=in"},
		Case{"foo.bar.unit=yes", "foo.bar.unit=Pckt.orig_unit=yes.pckt_type=sent.direction=in"},
		Case{"unit=yes.foo.bar", "unit=Pckt.foo.bar.orig_unit=yes.pckt_type=sent.direction=in"},
		Case{"target_type=count.foo.unit=ok.bar", "target_type=count.foo.unit=Pckt.bar.orig_unit=ok.pckt_type=sent.direction=in"},
	}
	for _, c := range cases {
		assert.Equal(t, Count_Pckt(c.in, "prefix."), c.out)
		c = Case{
			c.in,
			strings.Replace(strings.Replace(c.out, "unit=Pckt", "unit=Pcktps", -1), "target_type=count", "target_type=rate", -1),
		}
		assert.Equal(t, Rate_Pckt(c.in, "prefix."), c.out)
	}
	for _, c := range cases {
		c = Case{
			strings.Replace(c.in, "=", "_is_", -1),
			strings.Replace(c.out, "=", "_is_", -1),
		}
		assert.Equal(t, Count_Pckt(c.in, "prefix."), c.out)
		c = Case{
			c.in,
			strings.Replace(strings.Replace(c.out, "unit_is_Pckt", "unit_is_Pcktps", -1), "target_type_is_count", "target_type_is_rate", -1),
		}
		assert.Equal(t, Rate_Pckt(c.in, "prefix."), c.out)
	}
}

func BenchmarkManyDerive_Counts(t *testing.B) {
	for i := 0; i < 1000000; i++ {
		Derive_Count("foo.bar.unit=yes.baz", "prefix.")
		Derive_Count("foo.bar.unit=yes", "prefix.")
		Derive_Count("unit=yes.foo.bar", "prefix.")
		Derive_Count("foo.bar.unita=no.bar", "prefix.")
		Derive_Count("foo.bar.aunit=no.baz", "prefix.")
	}
}
