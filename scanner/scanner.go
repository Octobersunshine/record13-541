package scanner

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type ScanStatus string

const (
	StatusPending   ScanStatus = "pending"
	StatusRunning   ScanStatus = "running"
	StatusCompleted ScanStatus = "completed"
	StatusFailed    ScanStatus = "failed"
	StatusCanceled  ScanStatus = "canceled"
	StatusThrottled ScanStatus = "throttled"
	StatusPaused    ScanStatus = "paused"
)

type IOPriority int

const (
	PriorityLow      IOPriority = 0
	PriorityNormal   IOPriority = 1
	PriorityHigh     IOPriority = 2
)

type BadBlock struct {
	Sector   int64  `json:"sector"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Reason   string `json:"reason"`
}

type ProgressCallback func(current, total int64, percent float64, badBlocks []BadBlock, throttled bool)

type ThrottleConfig struct {
	MaxIOPS         int
	MaxBandwidthBps int64
	ScanSlice       time.Duration
	SleepSlice      time.Duration
	Priority        IOPriority
	EnableAdaptive  bool
}

type ScanConfig struct {
	DiskPath     string
	BlockSize    int64
	StartOffset  int64
	EndOffset    int64
	Speed        time.Duration
	Throttle     ThrottleConfig
}

type RateControlStats struct {
	ThrottleCount   int64         `json:"throttle_count"`
	TotalWaitTime   time.Duration `json:"total_wait_time"`
	EffectiveIOPS   float64       `json:"effective_iops"`
	EffectiveBW     float64       `json:"effective_bw_mbps"`
	LoadAvg         float64       `json:"system_load_avg"`
	PausedCount     int64         `json:"paused_count"`
}

type ScanResult struct {
	Status       ScanStatus       `json:"status"`
	TotalBlocks  int64            `json:"total_blocks"`
	Scanned      int64            `json:"scanned"`
	Percent      float64          `json:"percent"`
	BadBlocks    []BadBlock       `json:"bad_blocks"`
	Error        string           `json:"error,omitempty"`
	StartTime    time.Time        `json:"start_time"`
	EndTime      time.Time        `json:"end_time,omitempty"`
	Elapsed      string           `json:"elapsed"`
	RateStats    RateControlStats `json:"rate_stats"`
	IsThrottled  bool             `json:"is_throttled"`
}

type DiskScanner struct {
	taskID         string
	config         ScanConfig
	result         *ScanResult
	running        bool
	cancel         context.CancelFunc
	pauseCh        chan struct{}
	resumeCh       chan struct{}
	paused         bool
	rateLimiter    *RateLimiter
	timeSliceSched *TimeSliceScheduler
	lastProgressAt time.Time
	lastScanned    int64
}

func NewDiskScanner(config ScanConfig) *DiskScanner {
	if config.BlockSize == 0 {
		config.BlockSize = 4096
	}
	if config.EndOffset == 0 {
		config.EndOffset = 1024 * 1024 * 1024
	}
	if config.Throttle.MaxIOPS == 0 && config.Throttle.MaxBandwidthBps == 0 {
		config.Throttle.MaxIOPS = 200
		config.Throttle.MaxBandwidthBps = 50 * 1024 * 1024
	}
	if config.Throttle.ScanSlice == 0 {
		config.Throttle.ScanSlice = 200 * time.Millisecond
	}
	if config.Throttle.SleepSlice == 0 {
		config.Throttle.SleepSlice = 100 * time.Millisecond
	}

	ds := &DiskScanner{
		config: config,
		result: &ScanResult{
			Status:    StatusPending,
			BadBlocks: make([]BadBlock, 0),
		},
		pauseCh:  make(chan struct{}, 1),
		resumeCh: make(chan struct{}, 1),
	}
	return ds
}

func (ds *DiskScanner) SetTaskID(id string) {
	ds.taskID = id
}

func (ds *DiskScanner) Start(ctx context.Context, callback ProgressCallback) (*ScanResult, error) {
	if ds.running {
		return nil, fmt.Errorf("scanner is already running")
	}

	scanCtx, cancel := context.WithCancel(ctx)
	ds.cancel = cancel
	ds.running = true
	ds.lastProgressAt = time.Now()

	throttle := ds.config.Throttle
	maxIOPS := throttle.MaxIOPS
	maxBW := throttle.MaxBandwidthBps

	throttleObj := GetGlobalIOThrottle()
	throttleObj.IncrementActive()
	defer throttleObj.DecrementActive()

	if ds.taskID != "" && throttle.EnableAdaptive {
		allocIOPS, allocBW := throttleObj.RegisterTask(ds.taskID, maxIOPS, maxBW)
		maxIOPS = allocIOPS
		maxBW = allocBW
		defer throttleObj.UnregisterTask(ds.taskID)
	}

	switch throttle.Priority {
	case PriorityLow:
		maxIOPS = int(float64(maxIOPS) * 0.4)
		maxBW = int64(float64(maxBW) * 0.4)
	case PriorityHigh:
		maxIOPS = int(float64(maxIOPS) * 1.5)
		maxBW = int64(float64(maxBW) * 1.5)
	}

	ds.rateLimiter = NewRateLimiter(maxIOPS, maxBW)
	ds.timeSliceSched = NewTimeSliceScheduler(throttle.ScanSlice, throttle.SleepSlice)
	ds.timeSliceSched.Start(scanCtx)

	ds.result.Status = StatusRunning
	ds.result.StartTime = time.Now()
	ds.result.TotalBlocks = (ds.config.EndOffset - ds.config.StartOffset) / ds.config.BlockSize
	if ds.result.TotalBlocks <= 0 {
		ds.result.TotalBlocks = 1
	}

	startWait := time.Now()

	defer func() {
		ds.running = false
		ds.result.EndTime = time.Now()
		elapsed := ds.result.EndTime.Sub(ds.result.StartTime)
		ds.result.Elapsed = formatDuration(elapsed)
		ds.result.RateStats.TotalWaitTime += time.Since(startWait)

		if elapsed > 0 {
			ds.result.RateStats.EffectiveIOPS = float64(ds.result.Scanned) / elapsed.Seconds()
			ds.result.RateStats.EffectiveBW = float64(ds.result.Scanned*ds.config.BlockSize) / elapsed.Seconds() / (1024 * 1024)
		}
		ds.result.RateStats.LoadAvg = GetGlobalIOThrottle().GetSystemLoad().IOUtilization
	}()

	if err := ds.checkDiskAvailable(); err != nil {
		ds.result.Status = StatusFailed
		ds.result.Error = err.Error()
		return ds.result, err
	}

	loadCheckInterval := time.NewTicker(5 * time.Second)
	defer loadCheckInterval.Stop()

	for i := int64(0); i < ds.result.TotalBlocks; i++ {
		select {
		case <-scanCtx.Done():
			ds.result.Status = StatusCanceled
			ds.result.Error = "scan canceled by user"
			return ds.result, nil
		default:
		}

		select {
		case <-ds.pauseCh:
			ds.paused = true
			ds.result.RateStats.PausedCount++
			prevStatus := ds.result.Status
			ds.result.Status = StatusPaused
			if callback != nil {
				callback(ds.result.Scanned, ds.result.TotalBlocks, ds.result.Percent, ds.result.BadBlocks, false)
			}
			select {
			case <-scanCtx.Done():
				return ds.result, nil
			case <-ds.resumeCh:
				ds.paused = false
				ds.result.Status = prevStatus
			}
		default:
		}

		throttled := false
		gt := GetGlobalIOThrottle()
		if gt.IsEmergencyPaused() {
			throttled = true
			ds.result.IsThrottled = true
			ds.result.RateStats.ThrottleCount++
			if err := sleepWithCtx(scanCtx, 500*time.Millisecond); err != nil {
				return ds.result, nil
			}
			continue
		}

		if ds.timeSliceSched != nil {
			if err := ds.timeSliceSched.WaitIfSleeping(scanCtx); err != nil {
				return ds.result, nil
			}
		}

		if ds.rateLimiter != nil && ds.rateLimiter.IsEnabled() {
			waitStart := time.Now()
			if err := ds.rateLimiter.Wait(scanCtx, ds.config.BlockSize); err != nil {
				return ds.result, nil
			}
			waited := time.Since(waitStart)
			if waited > 5*time.Millisecond {
				throttled = true
				ds.result.RateStats.ThrottleCount++
				ds.result.RateStats.TotalWaitTime += waited
			}
		} else if ds.config.Speed > 0 {
			if err := sleepWithCtx(scanCtx, ds.config.Speed); err != nil {
				return ds.result, nil
			}
		}

		offset := ds.config.StartOffset + i*ds.config.BlockSize
		badBlock, err := ds.scanBlock(i, offset)
		if err != nil {
			ds.result.Status = StatusFailed
			ds.result.Error = err.Error()
			return ds.result, err
		}

		ds.result.Scanned = i + 1
		if badBlock != nil {
			ds.result.BadBlocks = append(ds.result.BadBlocks, *badBlock)
		}

		ds.result.Percent = float64(ds.result.Scanned) / float64(ds.result.TotalBlocks) * 100
		ds.result.IsThrottled = throttled
		if throttled && ds.result.Status == StatusRunning {
			ds.result.Status = StatusThrottled
		} else if !throttled && ds.result.Status == StatusThrottled {
			ds.result.Status = StatusRunning
		}

		if callback != nil {
			callback(ds.result.Scanned, ds.result.TotalBlocks, ds.result.Percent, ds.result.BadBlocks, throttled)
		}

		select {
		case <-loadCheckInterval.C:
			if throttle.EnableAdaptive && ds.taskID != "" {
				gt := GetGlobalIOThrottle()
				newIOPS, newBW := gt.RegisterTask(ds.taskID, ds.config.Throttle.MaxIOPS, ds.config.Throttle.MaxBandwidthBps)
				if ds.rateLimiter != nil {
					ds.rateLimiter.SetLimits(newIOPS, newBW)
				}
			}
		default:
		}
	}

	ds.result.Status = StatusCompleted
	ds.result.Percent = 100.0
	ds.result.IsThrottled = false
	return ds.result, nil
}

func (ds *DiskScanner) Cancel() {
	if ds.cancel != nil {
		ds.cancel()
	}
}

func (ds *DiskScanner) Pause() {
	select {
	case ds.pauseCh <- struct{}{}:
	default:
	}
}

func (ds *DiskScanner) Resume() {
	select {
	case ds.resumeCh <- struct{}{}:
	default:
	}
}

func (ds *DiskScanner) IsPaused() bool {
	return ds.paused
}

func (ds *DiskScanner) GetResult() *ScanResult {
	return ds.result
}

func (ds *DiskScanner) IsRunning() bool {
	return ds.running
}

func (ds *DiskScanner) UpdateRateLimits(maxIOPS int, maxBandwidthBps int64) {
	if ds.rateLimiter != nil {
		ds.rateLimiter.SetLimits(maxIOPS, maxBandwidthBps)
	}
}

func (ds *DiskScanner) UpdateTimeSlice(scanSlice, sleepSlice time.Duration) {
	if ds.timeSliceSched != nil {
		ds.timeSliceSched.SetDurations(scanSlice, sleepSlice)
	}
}

func (ds *DiskScanner) checkDiskAvailable() error {
	if ds.config.DiskPath == "" {
		return nil
	}
	_, err := os.Stat(ds.config.DiskPath)
	return err
}

func (ds *DiskScanner) scanBlock(blockIndex, offset int64) (*BadBlock, error) {
	simulatedData := byte(blockIndex % 256)
	_ = simulatedData

	if rand.Float64() < 0.005 {
		return &BadBlock{
			Sector: offset / 512,
			Offset: offset,
			Size:   ds.config.BlockSize,
			Reason: randomErrorReason(),
		}, nil
	}

	return nil, nil
}

func randomErrorReason() string {
	reasons := []string{
		"uncorrectable ECC error",
		"read timeout",
		"CRC mismatch",
		"media error",
		"address mark not found",
	}
	return reasons[rand.Intn(len(reasons))]
}

func sleepWithCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func formatDuration(d time.Duration) string {
	seconds := int(d.Seconds())
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, secs)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, secs)
	}
	return fmt.Sprintf("%ds", secs)
}
