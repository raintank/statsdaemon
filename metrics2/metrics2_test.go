package metrics2

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestDerive(t *testing.T) {
	assert.Equal(t, Derive("foo.bar.unit=yes.baz"), "foo.bar.unit=yesps.baz")
	assert.Equal(t, Derive("foo.bar.unit=yes"), "foo.bar.unit=yesps")
	assert.Equal(t, Derive("unit=yes.foo.bar"), "unit=yesps.foo.bar")
	assert.Equal(t, Derive("foo.bar.unita=no.bar"), "foo.bar.unita=no.bar")
	assert.Equal(t, Derive("foo.bar.aunit=no.baz"), "foo.bar.aunit=no.baz")
	assert.Equal(t, Derive("foo.bar.UNIT=no.baz"), "foo.bar.UNIT=no.baz")

	assert.Equal(t, Derive("foo.bar.target_type=count.baz"), "foo.bar.target_type=rate.baz")
	assert.Equal(t, Derive("foo.bar.target_type=count"), "foo.bar.target_type=rate")
	assert.Equal(t, Derive("target_type=count.foo.bar"), "target_type=rate.foo.bar")

}

func BenchmarkManyDerives(t *testing.B) {
	for i := 0; i < 1000000; i++ {
		Derive("foo.bar.unit=yes.baz")
		Derive("foo.bar.unit=yes")
		Derive("unit=yes.foo.bar")
		Derive("foo.bar.unita=no.bar")
		Derive("foo.bar.aunit=no.baz")
	}
}
