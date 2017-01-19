package out

type Formatter struct {
	// prefix of statsdaemon's own metrics2.0 stats
	PrefixInternal string

	// formatting of old style metrics
	Legacy_namespace bool
	Prefix_counters  string
	Prefix_gauges    string
	Prefix_rates     string
	Prefix_timers    string

	// formatting of metrics2.0
	Prefix_m20_counters string
	Prefix_m20_gauges   string
	Prefix_m20_rates    string
	Prefix_m20_timers   string

	// metrics2.0 using _is_ convention instead of =
	Prefix_m20ne_counters string
	Prefix_m20ne_gauges   string
	Prefix_m20ne_rates    string
	Prefix_m20ne_timers   string
}
