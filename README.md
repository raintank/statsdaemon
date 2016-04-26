statsdaemon
==========

Metrics aggregation daemon like [statsd](https://github.com/etsy/statsd), in Go and with a bunch of extra features.
(Based on code from [Bitly's statsdaemon](https://github.com/bitly/statsdaemon))

[![Build Status](https://secure.travis-ci.org/vimeo/statsdaemon.png)](http://travis-ci.org/vimeo/statsdaemon)
[![GoDoc](https://godoc.org/github.com/vimeo/statsdaemon?status.png)](https://godoc.org/github.com/vimeo/statsdaemon)


Features you expect:
=======================

For a given input, this implementation yields the exact same metrics as etsy's statsd (with legacy namespace and deleteIdleStats enabled),
so it can act as a drop-in replacement.  In terms of types:

* Timing (with optional percentiles, sampling supported)
* Counters (sampling supported)
* Gauges
* No histograms or sets yet, but should be easy to add if you want them


Metrics 2.0
===========

[metrics 2.0](http://dieter.plaetinck.be/metrics_2_a_proposal.html) is a format for structured, self-describing, standardized metrics.

![metrics 2.0 diagram](https://raw.github.com/vimeo/statsdaemon/master/img/metrics2.0-processor.png)

Metrics that flow through statsdaemon and are detected to be in the metrics 2.0 format undergo the same operations and aggregations, but how this is reflected in the resulting metric identifier is different:

* traditional/legacy metrics get prefixes and suffixes like the original statsd
* metrics in 2.0 format will have the appropriate adjustments to their tags.  Statsdaemon assures that tags such as unit, target_type, stat, etc reflect the performed operation, according to the [specification](https://github.com/vimeo/graph-explorer/wiki/Consistent-tag-keys-and-values).
This allows users and advanced tools such as [Graph-Explorer](http://vimeo.github.io/graph-explorer/) to truly understand metrics and leverage them.


Adaptive sampling
=================

TBA.


You'll love Go
==============

Perhaps debatable and prone to personal opinion, but people seem to agree that Go is more robust, easier to deploy and elegant than node.js.
In terms of performance, I didn't do extensive or scientific benchmarking but here's the effect on our cpu usage and calculation time when switching from statsd to statsdaemon, with the same input load and the same things being calculated:

![Performance](https://raw.github.com/vimeo/statsdaemon/master/img/statsd-to-statsdaemon-switch.png)

Performance and profiling
=========================

As with any statsd version, you should monitor whether the kernel drops incoming UDP packets.
When statsdaemon (or statsd) cannot read packets from the udp socket fast enough - perhaps because it's
overloaded with packet processing, or the udp reading is the slowest part of the chain (the
case in statsdaemon) - then the udp buffer will grow and ultimately fill up, and have no more room
for new packets, which get dropped, resulting in gaps in graphs.
With statsdaemon this limit seems to be at around 60k packets per second.
You can improve on this by batching multiple metrics into the same packet, and/or sampling more.
Statsdaemon exposes a profiling endpoint for pprof, at port 6060 by default (see config).

Admin telnet api
================

```
help                             show this menu
sample_rate <metric key>         for given metric, show:
                                 <key> <ideal sample rate> <Pckt/s sent (estim)>
metric_stats                     in the past 10s interval, for every metric show:
                                 <key> <Pckt/s sent (estim)> <Pckt/s received>
peek_valid                       stream all valid lines seen in real time
                                 until you disconnect or can't keep up.
peek_invalid                     stream all invalid lines seen in real time
                                 until you disconnect or can't keep up.
wait_flush                       after the next flush, writes 'flush' and closes connection.
                                 this is convenient to restart statsdaemon
                                 with a minimal loss of data like so:
                                 nc localhost 8126 <<< wait_flush && /sbin/restart statsdaemon
```


Internal metrics
================

Statsdaemon submits a bunch of internal performance metrics using itself.
Note that these metrics are in the [metrics 2.0 format](http://dieter.plaetinck.be/metrics_2_a_proposal.html),
they look a bit unusual but can be treated as regular graphite metrics if you want to.
However using [carbon-tagger](https://github.com/vimeo/carbon-tagger) and [Graph-Explorer](http://vimeo.github.io/graph-explorer/)
they become much more useful.


Installing
==========

```bash
go get github.com/Vimeo/statsdaemon/statsdaemon
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

legacyNamespace = true
prefix_rates = "stats."
prefix_counters = "stats_counts."
prefix_timers = "stats.timers."
prefix_gauges = "stats.gauges."

# Recommended (legacyNamespace = false)
# counts -> stats.counters.$metric.count
# rates -> stats.counters.$metric.rate

#legacyNamespace = false
#prefix_rates = "stats.counters."
#prefix_counters = "stats.counters."
#prefix_timers = "stats.timers."
#prefix_gauges = "stats.gauges."

percentile_thresholds = "90,75"
max_timers_per_s = 1000
```
