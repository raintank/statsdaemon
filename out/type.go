package out

import (
	"github.com/raintank/statsdaemon/common"
)

type Type interface {
	Add(metric *common.Metric)
	Process(buf []byte, now int64, interval int, f Formatter) ([]byte, int64)
}
