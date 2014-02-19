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

// the following functions change a metric string to represent a given operation
// if the metric is detected to be in metrics 2.0 format, the change
// will be in that style, if not, it will be a simple string prefix/postfix
// like legacy statsd.

func Derive_Count(metric_in, prefix string) (metric_out string) {
	is_metric20 := false
	parts := strings.Split(metric_in, ".")
	for i, part := range parts {
		if strings.HasPrefix(part, "unit=") {
			parts[i] = part + "ps"
			is_metric20 = true
		}
	}
	if is_metric20 {
		metric_out = strings.Join(parts, ".")
		metric_out = strings.Replace(metric_out, "target_type=count", "target_type=rate", 1)
	} else {
		metric_out = prefix + metric_in
	}
	return
}

func is_metric20(metric_in string) bool {
	if strings.Contains(metric_in, ".unit=") || strings.HasPrefix(metric_in, "unit=") {
		return true
	}
	return false
}

// metric 2.0: this operation doesn't really represent anything
// metric 1.0: prepend prefix
func Gauge(metric_in, prefix string) (metric_out string) {
	if is_metric20(metric_in) {
		return metric_in
	}
	return prefix + metric_in
}

func simple_stat(metric_in, prefix, stat, percentile string) (metric_out string) {
	if percentile != "" {
		percentile = "_" + percentile
	}
	if is_metric20(metric_in) {
		return metric_in + ".stat=" + stat + percentile
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
	is_metric20 := false
	parts := strings.Split(metric_in, ".")
	for i, part := range parts {
		if strings.HasPrefix(part, "unit=") {
			is_metric20 = true
			parts[i] = "unit=Pckt"
			parts = append(parts, "orig_unit="+part[5:])
		}
		if strings.HasPrefix(part, "target_type=") {
			is_metric20 = true
			parts[i] = "target_type=count"
		}
	}
	if is_metric20 {
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = prefix + metric_in + ".count"
	}
	return
}

func Rate_Pckt(metric_in, prefix string) (metric_out string) {
	is_metric20 := false
	parts := strings.Split(metric_in, ".")
	for i, part := range parts {
		if strings.HasPrefix(part, "unit=") {
			is_metric20 = true
			parts[i] = "unit=Pcktps"
			parts = append(parts, "orig_unit="+part[5:])
		}
		if strings.HasPrefix(part, "target_type=") {
			is_metric20 = true
			parts[i] = "target_type=rate"
		}
	}
	if is_metric20 {
		parts = append(parts, "pckt_type=sent")
		parts = append(parts, "direction=in")
		metric_out = strings.Join(parts, ".")
	} else {
		metric_out = prefix + metric_in + ".count_ps"
	}
	return
}
