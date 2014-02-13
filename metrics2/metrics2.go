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

// if a string metric is in the metrics2.0 format,
// update unit and target_type to represent deriving it's data
func Derive(metric_in string) (metric_out string) {
	parts := strings.Split(metric_in, ".")
	for i, part := range parts {
		if strings.HasPrefix(part, "unit=") {
			parts[i] = part + "ps"
		}
	}
	metric_out = strings.Join(parts, ".")
	metric_out = strings.Replace(metric_out, "target_type=count", "target_type=rate", 1)
	return
}
