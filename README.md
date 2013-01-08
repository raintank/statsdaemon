gographite
==========

Port of Etsy's statsd (https://github.com/etsy/statsd), written in Go.

* supports Timing, Counters, Gauges, Sampling
* supports percentage values

[![Build Status](https://secure.travis-ci.org/bitly/gographite.png)](http://travis-ci.org/bitly/gographite)

Command Line Options
====================

```
Usage of ./gographite:
  -address=":8125": UDP service address
  -debug=false: print statistics sent to graphite
  -flush-interval=10: Flush interval (seconds)
  -graphite="127.0.0.1:2003": Graphite service address (or - to disable)
  -percent-threshold=[]: Threshold percent (may be given multiple times)
  -persist-count-keys=60: number of flush-interval's to persist count keys
  -version=false: print version string
```
