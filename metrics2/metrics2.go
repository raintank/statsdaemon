// Package metrics2 provides functions that manipulate a metric string to represent a given operation
// if the metric is detected to be in metrics 2.0 format, the change
// will be in that style, if not, it will be a simple string prefix/postfix
// like legacy statsd.
package metrics2

import (
	"strings"
)

/*
I can't get the regex approach to work
the split-fix-join method might be faster anyway

func fix2(s string) {
    re := regexp.MustCompile("((^|\\.)unit=[^\\.]*)(\\.|$)")
    fmt.Println(s, "       ", re.ReplaceAllString(s, "${1}ps${2}"))
}
*/

type metricVersion int

const (
	legacy metricVersion = iota
	m20
	m20NoEquals
)

func getVersion(metric_in string) metricVersion {
	if strings.Contains(metric_in, "unit=") {
		return m20
	}
	if strings.Contains(metric_in, "unit_is_") {
		return m20NoEquals
	}
	return legacy
}

func is_metric20(metric_in string) bool {
	v := getVersion(metric_in)
	return v == m20 || v == m20NoEquals
}

// Derive_Count represents a derive from counter to rate per second
func Derive_Count(metric_in, prefix string) (metric_out string) {
	if is_metric20(metric_in) {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") || strings.HasPrefix(part, "unit_is_") {
				parts[i] = part + "ps"
			}
		}
		metric_out = strings.Join(parts, ".")
		metric_out = strings.Replace(metric_out, "target_type=count", "target_type=rate", 1)
		metric_out = strings.Replace(metric_out, "target_type_is_count", "target_type_is_rate", 1)
	} else {
		metric_out = prefix + metric_in
	}
	return
}

// Gauge doesn't really represent a change in data format, so for metrics 2.0 it doesn't change anything
func Gauge(metric_in, prefix string) (metric_out string) {
	if is_metric20(metric_in) {
		return metric_in
	}
	return prefix + metric_in
}

// simple_stat is a helper function to help express some common statistical aggregations using the stat tag
// with an optional percentile
func simple_stat(metric_in, prefix, stat, percentile string) (metric_out string) {
	if percentile != "" {
		percentile = "_" + percentile
	}
	v := getVersion(metric_in)
	if v == m20 {
		return metric_in + ".stat=" + stat + percentile
	}
	if v == m20NoEquals {
		return metric_in + ".stat_is_" + stat + percentile
	}
	return prefix + metric_in + "." + stat + percentile
}

func Upper(metric_in, prefix, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "upper", percentile)
}

func Lower(metric_in, prefix, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "lower", percentile)
}

func Mean(metric_in, prefix, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "mean", percentile)
}

func Sum(metric_in, prefix, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "sum", percentile)
}

func Median(metric_in, prefix string, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "median", percentile)
}

func Std(metric_in, prefix string, percentile string) (metric_out string) {
	return simple_stat(metric_in, prefix, "std", percentile)
}

func Count_Pckt(metric_in, prefix string) (metric_out string) {
	v := getVersion(metric_in)
	if v == m20 {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pckt"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "target_type=") {
				parts[i] = "target_type=count"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else if v == m20NoEquals {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pckt"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "target_type_is_") {
				parts[i] = "target_type_is_count"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = prefix + metric_in + ".count"
	}
	return
}

func Rate_Pckt(metric_in, prefix string) (metric_out string) {
	v := getVersion(metric_in)
	if v == m20 {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit=") {
				parts[i] = "unit=Pcktps"
				parts = append(parts, "orig_unit="+part[5:])
			}
			if strings.HasPrefix(part, "target_type=") {
				parts[i] = "target_type=rate"
			}
		}
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else if v == m20NoEquals {
		parts := strings.Split(metric_in, ".")
		for i, part := range parts {
			if strings.HasPrefix(part, "unit_is_") {
				parts[i] = "unit_is_Pcktps"
				parts = append(parts, "orig_unit_is_"+part[8:])
			}
			if strings.HasPrefix(part, "target_type_is_") {
				parts[i] = "target_type_is_rate"
			}
		}
		parts = append(parts, "pckt_type_is_sent")
		parts = append(parts, "direction_is_in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = prefix + metric_in + ".count_ps"
	}
	return
}
