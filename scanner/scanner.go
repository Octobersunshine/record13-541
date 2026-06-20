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
	StatusPending    ScanStatus = "pending"
	StatusRunning    ScanStatus = "running"
	StatusCompleted  ScanStatus = "completed"
	StatusFailed     ScanStatus = "failed"
	StatusCanceled   ScanStatus = "canceled"
	StatusThrottled  ScanStatus = "throttled"
	StatusPaused     ScanStatus = "paused"
	StatusWarmingUp  ScanStatus = "warming_up"
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
	FullThrottle CompleteThrottleConfig
	UsePreset    bool
	PresetName   PresetProfile
}

type ReadControlStats struct {
	TotalReads          int64         `json:"total_reads"`
	BytesRead           int64         `json:"bytes_read_mb"`
	ConsecutiveReadCount int64        `json:"consecutive_read_breaks"`
	IntervalSleepCount  int64         `json:"interval_sleep_count"`
	AfterReadSleepCount int64         `json:"after_read_sleep_count"`
	WarmupPhaseMs       int64         `json:"warmup_phase_ms"`
	PeriodCooldownCount int64         `json:"period_cooldown_count"`
}

type RateControlStats struct {
	ThrottleCount   int64         `json:"throttle_count"`
	TotalWaitTime   time.Duration `json:"total_wait_time"`
	EffectiveIOPS   float64       `json:"effective_iops"`
	EffectiveBW     float64       `json:"effective_bw_mbps"`
	LoadAvg         float64       `json:"system_load_avg"`
	PausedCount     int64         `json:"paused_count"`
	ReadControl     ReadControlStats `json:"read_control"`
	LimitConfig     map[string]interface{} `json:"limit_config"`
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
	PresetUsed   string           `json:"preset_used,omitempty"`
}

type DiskScanner struct {
	taskID              string
	config              ScanConfig
	result              *ScanResult
	running             bool
	cancel              context.CancelFunc
	pauseCh             chan struct{}
	resumeCh            chan struct{}
	paused              bool
	rateLimiter         *RateLimiter
	timeSliceSched      *TimeSliceScheduler
	warmupCtrl          *WarmupController
	lastProgressAt      time.Time
	lastScanned         int64
	consecutiveReads    int
	lastReadAt          time.Time
	periodTrackers      []*periodTracker
}

type periodTracker struct {
	config      IOPeriodLimit
	windowStart time.Time
	readsInWin  int64
	bytesInWin  int64
	inCooldown  bool
	cooldownEnd time.Time
}

func NewDiskScanner(config ScanConfig) *DiskScanner {
	if config.BlockSize == 0 {
		config.BlockSize = 4096
	}
	if config.EndOffset == 0 {
		config.EndOffset = 1024 * 1024 * 1024
	}

	if config.UsePreset && config.PresetName != "" {
		full := GetPresetConfig(config.PresetName)
		config.FullThrottle = full
		config.Throttle = full.ToScanThrottleConfig()
	} else if config.Throttle.MaxIOPS == 0 && config.Throttle.MaxBandwidthBps == 0 {
		full := GetPresetConfig(ProfileConservative)
		config.FullThrottle = full
		config.Throttle = full.ToScanThrottleConfig()
		config.PresetName = ProfileConservative
		config.UsePreset = true
	}

	if config.Throttle.ScanSlice == 0 {
		config.Throttle.ScanSlice = 200 * time.Millisecond
	}
	if config.Throttle.SleepSlice == 0 {
		config.Throttle.SleepSlice = 100 * time.Millisecond
	}

	rc := ReadControlStats{}
	rateStats := RateControlStats{
		ReadControl: rc,
		LimitConfig: config.FullThrottle.Summary(),
	}

	ds := &DiskScanner{
		config: config,
		result: &ScanResult{
			Status:     StatusPending,
			BadBlocks:  make([]BadBlock, 0),
			RateStats:  rateStats,
			PresetUsed: string(config.PresetName),
		},
		pauseCh:  make(chan struct{}, 1),
		resumeCh: make(chan struct{}, 1),
	}

	if config.FullThrottle.BehaviorConfig.EnablePeriodLimits &&
		len(config.FullThrottle.PeriodLimits) > 0 {
		for _, p := range config.FullThrottle.PeriodLimits {
			ds.periodTrackers = append(ds.periodTrackers, &periodTracker{
				config:      p,
				windowStart: time.Now(),
			})
		}
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
	ds.lastReadAt = time.Now()

	throttle := ds.config.Throttle
	fullCfg := ds.config.FullThrottle
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

	ds.warmupCtrl = NewWarmupController(fullCfg.BehaviorConfig, int64(maxIOPS), maxBW)

	initialRPS, initialBPS := ds.warmupCtrl.CurrentLimits()
	if ds.warmupCtrl.IsWarmingUp() {
		ds.rateLimiter = NewRateLimiter(int(initialRPS), initialBPS)
	} else {
		ds.rateLimiter = NewRateLimiter(maxIOPS, maxBW)
	}

	ds.timeSliceSched = NewTimeSliceScheduler(throttle.ScanSlice, throttle.SleepSlice)
	ds.timeSliceSched.Start(scanCtx)

	ds.result.Status = StatusRunning
	if ds.warmupCtrl.IsWarmingUp() {
		ds.result.Status = StatusWarmingUp
	}
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

		rc := ds.result.RateStats.ReadControl
		rc.TotalReads = ds.result.Scanned
		rc.BytesRead = ds.result.Scanned * ds.config.BlockSize / (1024 * 1024)
		ds.result.RateStats.ReadControl = rc

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

	warmupCheckInterval := time.NewTicker(200 * time.Millisecond)
	defer warmupCheckInterval.Stop()

	readCfg := fullCfg.ReadRate

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

		for _, pt := range ds.periodTrackers {
			if pt.inCooldown {
				if time.Now().Before(pt.cooldownEnd) {
					throttled = true
					ds.result.RateStats.ThrottleCount++
					ds.result.RateStats.ReadControl.PeriodCooldownCount++
					remain := time.Until(pt.cooldownEnd)
					if remain > 100*time.Millisecond {
						remain = 100 * time.Millisecond
					}
					if err := sleepWithCtx(scanCtx, remain); err != nil {
						return ds.result, nil
					}
					continue
				}
				pt.inCooldown = false
				pt.windowStart = time.Now()
				pt.readsInWin = 0
				pt.bytesInWin = 0
			}
		}

		select {
		case <-warmupCheckInterval.C:
			if ds.warmupCtrl.IsWarmingUp() {
				curRPS, curBPS := ds.warmupCtrl.CurrentLimits()
				ds.rateLimiter.SetLimits(int(curRPS), curBPS)
			} else if ds.result.Status == StatusWarmingUp {
				ds.result.Status = StatusRunning
				ds.rateLimiter.SetLimits(maxIOPS, maxBW)
			}
		default:
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

		if readCfg.MinReadIntervalUs > 0 {
			elapsed := time.Since(ds.lastReadAt)
			minInterval := time.Duration(readCfg.MinReadIntervalUs) * time.Microsecond
			if elapsed < minInterval {
				waitTime := minInterval - elapsed
				throttled = true
				ds.result.RateStats.ReadControl.IntervalSleepCount++
				ds.result.RateStats.TotalWaitTime += waitTime
				if err := sleepWithCtx(scanCtx, waitTime); err != nil {
					return ds.result, nil
				}
			}
		}

		if readCfg.MaxConsecutiveReads > 0 {
			ds.consecutiveReads++
			if ds.consecutiveReads >= readCfg.MaxConsecutiveReads {
				ds.consecutiveReads = 0
				sleepUs := int64(2000)
				if readCfg.AfterReadSleepUs > 0 {
					sleepUs = readCfg.AfterReadSleepUs
				}
				throttled = true
				ds.result.RateStats.ReadControl.ConsecutiveReadCount++
				sleepDur := time.Duration(sleepUs) * time.Microsecond
				ds.result.RateStats.TotalWaitTime += sleepDur
				if err := sleepWithCtx(scanCtx, sleepDur); err != nil {
					return ds.result, nil
				}
			}
		} else if readCfg.AfterReadSleepUs > 0 {
			sleepDur := time.Duration(readCfg.AfterReadSleepUs) * time.Microsecond
			throttled = true
			ds.result.RateStats.ReadControl.AfterReadSleepCount++
			ds.result.RateStats.TotalWaitTime += sleepDur
			if err := sleepWithCtx(scanCtx, sleepDur); err != nil {
				return ds.result, nil
			}
		}

		ds.lastReadAt = time.Now()

		offset := ds.config.StartOffset + i*ds.config.BlockSize
		badBlock, err := ds.scanBlock(i, offset)
		if err != nil {
			ds.result.Status = StatusFailed
			ds.result.Error = err.Error()
			return ds.result, err
		}

		for _, pt := range ds.periodTrackers {
			pt.readsInWin++
			pt.bytesInWin += ds.config.BlockSize
			if time.Since(pt.windowStart) >= pt.config.WindowDuration {
				if (pt.config.MaxReadsInWindow > 0 && pt.readsInWin > pt.config.MaxReadsInWindow) ||
					(pt.config.MaxBytesInWindow > 0 && pt.bytesInWin > pt.config.MaxBytesInWindow) {
					pt.inCooldown = true
					pt.cooldownEnd = time.Now().Add(pt.config.CooldownAfterWindow)
				} else {
					pt.windowStart = time.Now()
					pt.readsInWin = 0
					pt.bytesInWin = 0
				}
			}
		}

		ds.result.Scanned = i + 1
		if badBlock != nil {
			ds.result.BadBlocks = append(ds.result.BadBlocks, *badBlock)
		}

		ds.result.Percent = float64(ds.result.Scanned) / float64(ds.result.TotalBlocks) * 100
		ds.result.IsThrottled = throttled

		if ds.warmupCtrl.IsWarmingUp() {
			ds.result.Status = StatusWarmingUp
		} else if throttled && ds.result.Status == StatusRunning {
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
				switch throttle.Priority {
				case PriorityLow:
					newIOPS = int(float64(newIOPS) * 0.4)
					newBW = int64(float64(newBW) * 0.4)
				case PriorityHigh:
					newIOPS = int(float64(newIOPS) * 1.5)
					newBW = int64(float64(newBW) * 1.5)
				}
				if !ds.warmupCtrl.IsWarmingUp() {
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
