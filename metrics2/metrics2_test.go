package metrics2

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestDerive_Count(t *testing.T) {
	assert.Equal(t, Derive_Count("foo.bar.unit=yes.baz", "prefix."), "foo.bar.unit=yesps.baz")
	assert.Equal(t, Derive_Count("foo.bar.unit=yes", "prefix."), "foo.bar.unit=yesps")
	assert.Equal(t, Derive_Count("unit=yes.foo.bar", "prefix."), "unit=yesps.foo.bar")
	assert.Equal(t, Derive_Count("foo.bar.unita=no.bar", "prefix."), "prefix.foo.bar.unita=no.bar")
	assert.Equal(t, Derive_Count("foo.bar.aunit=no.baz", "prefix."), "prefix.foo.bar.aunit=no.baz")
	assert.Equal(t, Derive_Count("foo.bar.UNIT=no.baz", "prefix."), "prefix.foo.bar.UNIT=no.baz")

	// only update target_type if we noticed it's metrics 2.0
	assert.Equal(t, Derive_Count("foo.bar.target_type=count.baz", "prefix."), "prefix.foo.bar.target_type=count.baz")
	assert.Equal(t, Derive_Count("foo.bar.target_type=count", "prefix."), "prefix.foo.bar.target_type=count")
	assert.Equal(t, Derive_Count("target_type=count.foo.bar", "prefix."), "prefix.target_type=count.foo.bar")
	assert.Equal(t, Derive_Count("target_type=count.foo.unit=ok.bar", "prefix."), "target_type=rate.foo.unit=okps.bar")

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
