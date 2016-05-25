package cpu

import (
	"fmt"
	"github.com/shirou/gopsutil/process"
	"os"
	"runtime/pprof"
	"time"
)

// Cpu will check every checkEvery for cpu percentage used to reach or exceed the threshold and take a cpu profile to the path directory,
// but no more often than every minTimeDiff seconds
// any errors will be sent to the errors channel
// the duration of the cpu profile is controlled via  profDur.
type Cpu struct {
	path        string
	threshold   int
	minTimeDiff int
	checkEvery  time.Duration
	profDur     time.Duration
	lastUnix    int64
	Errors      chan error
}

// New creates a new Cpu trigger. use a nil channel if you don't care about any errors
func New(path string, threshold, minTimeDiff int, checkEvery, profDur time.Duration, errors chan error) (*Cpu, error) {
	cpu := Cpu{
		path,
		threshold,
		minTimeDiff,
		checkEvery,
		profDur,
		int64(0),
		errors,
	}
	return &cpu, nil
}

func (cpu Cpu) logError(err error) {
	if cpu.Errors != nil {
		cpu.Errors <- err
	}
}

// Run runs the trigger. encountered errors go to the configured channel (if any).
// you probably want to run this in a new goroutine.
func (cpu Cpu) Run() {
	tick := time.NewTicker(cpu.checkEvery)
	pid := os.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		cpu.logError(err)
		return
	}
	for ts := range tick.C {
		percent, err := p.Percent(0)
		if err != nil {
			cpu.logError(err)
			continue
		}
		//fmt.Println("percent is now", percent)
		unix := ts.Unix()
		// we discard the decimals of the percentage. an integer with percent resolution should be good enough.
		if int(percent) >= cpu.threshold && unix >= cpu.lastUnix+int64(cpu.minTimeDiff) {
			f, err := os.Create(fmt.Sprintf("%s/%d.profile-cpu", cpu.path, unix))
			if err != nil {
				cpu.logError(err)
				continue
			}
			err = pprof.StartCPUProfile(f)
			if err != nil {
				cpu.logError(err)
			}
			time.Sleep(cpu.profDur)
			pprof.StopCPUProfile()
			cpu.lastUnix = unix
			err = f.Close()
			if err != nil {
				cpu.logError(err)
			}
		}
	}
}
