statsdaemon
==========

Port of Etsy's statsd (https://github.com/etsy/statsd), written in Go.
(Originally based on [Bitly's statsdaemon](https://github.com/bitly/statsdaemon)
but heavily extended)


features you'll expect:
=======================

For a given input, this implementation yields the exact same metrics as etsy's statsd (with legacy namespace and deleteGauges enabled),
so it can act as a drop-in replacement.

Supports:

* Timing (with optional percentiles, sampling supported)
* Counters (sampling supported)
* Gauges

No support yet for histograms or sets (barely anyone uses them), but should be easy to add.


Metrics 2.0
===========

[metrics 2.0](http://dieter.plaetinck.be/metrics_2_a_proposal.html) is a format for structured, self-describing, standardized metrics.

Metrics that flow through statsdaemon and are detected to be in the metrics 2.0 format undergo the same operations and aggregations, but how this is reflected in the resulting metric identifier is different:

* legacy ("old school" statsd/graphite metrics) get unstandard prefixes and suffixes like the original statsd
* metrics in 2.0 format will have the appropriate adjustments to their tags.  Statsdaemon assures that tags such as unit, target_type, stat, etcreflect the performed operation, according to the [specification](https://github.com/vimeo/graph-explorer/wiki/Consistent-tag-keys-and-values).
This allows users and advanced tools such as [Graph-Explorer](http://vimeo.github.io/graph-explorer/) to properly understand metrics and leverage them.


Adaptive sampling
=================

TBA.

[![Build Status](https://secure.travis-ci.org/Vimeo/statsdaemon.png)](http://travis-ci.org/Vimeo/statsdaemon)


Admin telnet api
================

```
        sample_rate <metric key>         for given metric, show:
                                         <key> <ideal sample rate> <Pckt/s sent (estim)>
        help                             show this menu
        metric_stats                     in the past 10s interval, for every metric show:
                                         <key> <Pckt/s sent (estim)> <Pckt/s received>
```


Internal metrics
================

Statsdaemon submits a bunch of internal performance metrics;
it does this using itself so the metrics are also subject to the chosen prefixes.
Note that these metrics are in the [metrics 2.0 format](http://dieter.plaetinck.be/metrics_2_a_proposal.html),
they look a bit unusual but can be treated as regular graphite metrics if you want to.
However using [carbon-tagger](https://github.com/vimeo/carbon-tagger) and [Graph-Explorer](http://vimeo.github.io/graph-explorer/)
they become much more useful.


Installing
==========

```bash
go get github.com/Vimeo/statsdaemon
```

Command Line Options
====================

```
Usage of ./statsdaemon:
  -config_file="/etc/statsdaemon.ini": config file location
  -cpuprofile="": write cpu profile to file
  -debug=false: print statistics sent to graphite
  -memprofile="": write memory profile to this file
  -version=false: print version string
```

Config file options
===================
```
listen_addr = ":8125"
admin_addr = ":8126"
graphite_addr = "127.0.0.1:2003"
flush_interval = 60

prefix_rates = "stats."
prefix_timers = "stats.timers."
prefix_gauges = "stats.gauges."

percentile_tresholds = "90,75"
max_timers_per_s = 1000
```
