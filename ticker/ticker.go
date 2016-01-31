package ticker

import (
	"github.com/benbjohnson/clock"
	"time"
)

/*
func main() {
    work := 0
    for {
        ticker := GetAlignedTicker(time.Duration(1) * time.Second)
        <- ticker.C
        fmt.Println("starting work at", time.Now())
        time.Sleep(time.Duration(work) * 5 * time.Millisecond)
        fmt.Println("    done work at", time.Now())
        work += 1
    }
}
*/

// GetAlignedTicker returns a ticker so that, let's say interval is a second
// then it will tick at every whole second, or if it's 60s than it's every whole
// minute. Note that in my testing this is about .0001 to 0.0002 seconds off due
// to scheduling etc.
func GetAlignedTicker(c clock.Clock, period time.Duration) *clock.Ticker {
	unix := c.Now().UnixNano()
	diff := time.Duration(period - (time.Duration(unix) % period))
	return c.Ticker(diff)
}
