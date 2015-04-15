package timers

import (
	"fmt"
	"strconv"
	"strings"
)

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

func NewPercentile(pctl string) (*Percentile, error) {
	f, err := strconv.ParseFloat(pctl, 64)
	if err != nil {
		return nil, err
	}
	return &Percentile{f, strings.Replace(pctl, ".", "_", -1)}, nil
}

func NewPercentiles(pctls string) (*Percentiles, error) {
	percentiles := Percentiles{}
	pcts := strings.Split(pctls, ",")
	for _, pct := range pcts {
		if pct == "" {
			continue
		}
		percentile, err := NewPercentile(pct)
		if err != nil {
			return nil, err
		}
		percentiles = append(percentiles, percentile)
	}
	return &percentiles, nil
}
