//go:build !windows

package proc

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	cpuMu       sync.Mutex
	prevTotal   uint64
	prevIdle    uint64
	hasPrevCPU  bool
	lastCPUPct  float64
)

// GetCPUUsagePct returns the host CPU utilization percentage using /proc/stat deltas.
func GetCPUUsagePct() float64 {
	cpuMu.Lock()
	defer cpuMu.Unlock()

	total, idle, ok := readProcStat()
	if !ok {
		return lastCPUPct
	}
	if !hasPrevCPU {
		prevTotal, prevIdle, hasPrevCPU = total, idle, true
		time.Sleep(50 * time.Millisecond)
		if t2, i2, ok2 := readProcStat(); ok2 {
			total, idle = t2, i2
		} else {
			return 0
		}
	}

	deltaTotal := total - prevTotal
	deltaIdle := idle - prevIdle
	prevTotal, prevIdle = total, idle

	if deltaTotal == 0 {
		return lastCPUPct
	}
	usage := (float64(deltaTotal-deltaIdle) / float64(deltaTotal)) * 100.0
	lastCPUPct = math.Round(math.Max(0, math.Min(100, usage))*10) / 10
	return lastCPUPct
}

func readProcStat() (uint64, uint64, bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			return 0, 0, false
		}
		var total, idle uint64
		for i, f := range fields[1:] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				continue
			}
			total += v
			if i == 3 || i == 4 { // idle + iowait
				idle += v
			}
		}
		return total, idle, true
	}
	return 0, 0, false
}

// SetHideWindow is a no-op on non-Windows platforms.
func SetHideWindow(cmd *exec.Cmd) *exec.Cmd {
	return cmd
}

// GetFFmpegPath returns ffmpeg binary on unix, prioritizing high-performance local builds.
func GetFFmpegPath() string {
	candidates := []string{
		"./bin/ffmpeg",
		"bin/ffmpeg",
		"../HydraStream/bin/ffmpeg",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			if abs, err := filepath.Abs(c); err == nil {
				return abs
			}
			return c
		}
	}
	if p, err := exec.LookPath("ffmpeg"); err == nil {
		return p
	}
	return "ffmpeg"
}

// GetFFprobePath returns ffprobe binary on unix, prioritizing local builds.
func GetFFprobePath() string {
	candidates := []string{
		"./bin/ffprobe",
		"bin/ffprobe",
		"../HydraStream/bin/ffprobe",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			if abs, err := filepath.Abs(c); err == nil {
				return abs
			}
			return c
		}
	}
	if p, err := exec.LookPath("ffprobe"); err == nil {
		return p
	}
	return "ffprobe"
}
