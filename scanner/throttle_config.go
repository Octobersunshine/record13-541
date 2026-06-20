package scanner

import (
	"fmt"
	"time"
)

type PresetProfile string

const (
	ProfileUltraSafe  PresetProfile = "ultra_safe"
	ProfileConservative PresetProfile = "conservative"
	ProfileBalanced   PresetProfile = "balanced"
	ProfileAggressive PresetProfile = "aggressive"
	ProfileNightOnly  PresetProfile = "night_only"
)

type ReadRateLimit struct {
	ReadsPerSecond      int64
	BytesPerSecond      int64
	MaxReadBurstBytes   int64
	MinReadIntervalUs   int64
	MaxConsecutiveReads int
	AfterReadSleepUs    int64
}

type IOPeriodLimit struct {
	WindowDuration     time.Duration
	MaxReadsInWindow   int64
	MaxBytesInWindow   int64
	CooldownAfterWindow time.Duration
}

type CompleteThrottleConfig struct {
	Preset               PresetProfile
	ReadRate             ReadRateLimit
	TimeSlice            TimeSliceConfig
	GlobalLimits         GlobalIOConfig
	BehaviorConfig       BehaviorConfig
	PeriodLimits         []IOPeriodLimit
	OverrideDefaults     bool
}

type TimeSliceConfig struct {
	ScanDuration  time.Duration
	SleepDuration time.Duration
	AutoScaleDuty bool
}

type GlobalIOConfig struct {
	MaxConcurrentTasks  int64
	TotalMaxIOPS        int64
	TotalMaxBandwidth   int64
	ReserveForBusiness  struct {
		MinFreeIOPS       int64
		MinFreeBandwidth  int64
		MaxUtilizationPct float64
	}
}

type BehaviorConfig struct {
	StartupWarmupMs      int64
	WarmupStartPct       float64
	EmergencyPauseOnHigh bool
	PauseOnErrorCount    int
	AutoResumeAfterMs    int64
	Priority             IOPriority
	EnableAdaptive       bool
	EnablePeriodLimits   bool
}

func DefaultThrottleConfig() CompleteThrottleConfig {
	return CompleteThrottleConfig{
		Preset: ProfileConservative,
		ReadRate: ReadRateLimit{
			ReadsPerSecond:      200,
			BytesPerSecond:      50 * 1024 * 1024,
			MaxReadBurstBytes:   1 * 1024 * 1024,
			MinReadIntervalUs:   0,
			MaxConsecutiveReads: 50,
			AfterReadSleepUs:    0,
		},
		TimeSlice: TimeSliceConfig{
			ScanDuration:   200 * time.Millisecond,
			SleepDuration:  100 * time.Millisecond,
			AutoScaleDuty:  true,
		},
		GlobalLimits: func() GlobalIOConfig {
			g := GlobalIOConfig{
				MaxConcurrentTasks: 2,
				TotalMaxIOPS:       500,
				TotalMaxBandwidth:  100 * 1024 * 1024,
			}
			g.ReserveForBusiness.MinFreeIOPS = 200
			g.ReserveForBusiness.MinFreeBandwidth = 50 * 1024 * 1024
			g.ReserveForBusiness.MaxUtilizationPct = 0.35
			return g
		}(),
		BehaviorConfig: BehaviorConfig{
			StartupWarmupMs:      5000,
			WarmupStartPct:       0.3,
			EmergencyPauseOnHigh: true,
			PauseOnErrorCount:    10,
			AutoResumeAfterMs:    0,
			Priority:             PriorityLow,
			EnableAdaptive:       true,
			EnablePeriodLimits:   false,
		},
		PeriodLimits:         nil,
		OverrideDefaults:     false,
	}
}

func GetPresetConfig(profile PresetProfile) CompleteThrottleConfig {
	cfg := DefaultThrottleConfig()
	cfg.Preset = profile
	cfg.OverrideDefaults = true

	switch profile {
	case ProfileUltraSafe:
		cfg.ReadRate.ReadsPerSecond = 30
		cfg.ReadRate.BytesPerSecond = 5 * 1024 * 1024
		cfg.ReadRate.MinReadIntervalUs = 20000
		cfg.ReadRate.MaxConsecutiveReads = 10
		cfg.ReadRate.AfterReadSleepUs = 5000
		cfg.TimeSlice.ScanDuration = 100 * time.Millisecond
		cfg.TimeSlice.SleepDuration = 500 * time.Millisecond
		cfg.BehaviorConfig.Priority = PriorityLow
		cfg.BehaviorConfig.EnableAdaptive = true
		cfg.GlobalLimits.ReserveForBusiness.MaxUtilizationPct = 0.15

	case ProfileConservative:
		cfg.ReadRate.ReadsPerSecond = 80
		cfg.ReadRate.BytesPerSecond = 15 * 1024 * 1024
		cfg.ReadRate.MinReadIntervalUs = 5000
		cfg.ReadRate.MaxConsecutiveReads = 20
		cfg.TimeSlice.ScanDuration = 150 * time.Millisecond
		cfg.TimeSlice.SleepDuration = 350 * time.Millisecond
		cfg.BehaviorConfig.Priority = PriorityLow
		cfg.BehaviorConfig.EnableAdaptive = true
		cfg.GlobalLimits.ReserveForBusiness.MaxUtilizationPct = 0.25

	case ProfileBalanced:
		cfg.ReadRate.ReadsPerSecond = 200
		cfg.ReadRate.BytesPerSecond = 50 * 1024 * 1024
		cfg.ReadRate.MaxConsecutiveReads = 50
		cfg.TimeSlice.ScanDuration = 200 * time.Millisecond
		cfg.TimeSlice.SleepDuration = 100 * time.Millisecond
		cfg.BehaviorConfig.Priority = PriorityNormal
		cfg.BehaviorConfig.EnableAdaptive = true
		cfg.GlobalLimits.ReserveForBusiness.MaxUtilizationPct = 0.40

	case ProfileAggressive:
		cfg.ReadRate.ReadsPerSecond = 500
		cfg.ReadRate.BytesPerSecond = 150 * 1024 * 1024
		cfg.ReadRate.MaxConsecutiveReads = 200
		cfg.TimeSlice.ScanDuration = 400 * time.Millisecond
		cfg.TimeSlice.SleepDuration = 50 * time.Millisecond
		cfg.BehaviorConfig.Priority = PriorityHigh
		cfg.BehaviorConfig.EnableAdaptive = false
		cfg.GlobalLimits.ReserveForBusiness.MaxUtilizationPct = 0.65

	case ProfileNightOnly:
		cfg.ReadRate.ReadsPerSecond = 800
		cfg.ReadRate.BytesPerSecond = 300 * 1024 * 1024
		cfg.ReadRate.MaxConsecutiveReads = 500
		cfg.TimeSlice.ScanDuration = 500 * time.Millisecond
		cfg.TimeSlice.SleepDuration = 0
		cfg.BehaviorConfig.Priority = PriorityHigh
		cfg.BehaviorConfig.EnableAdaptive = false
		cfg.BehaviorConfig.StartupWarmupMs = 0
		cfg.GlobalLimits.ReserveForBusiness.MaxUtilizationPct = 0.85
	}

	cfg.ApplyPeriodLimits(profile)
	return cfg
}

func (c *CompleteThrottleConfig) ApplyPeriodLimits(profile PresetProfile) {
	switch profile {
	case ProfileConservative, ProfileBalanced:
		c.PeriodLimits = []IOPeriodLimit{
			{
				WindowDuration:      time.Minute,
				MaxReadsInWindow:    c.ReadRate.ReadsPerSecond * 45,
				MaxBytesInWindow:    c.ReadRate.BytesPerSecond * 45,
				CooldownAfterWindow: 15 * time.Second,
			},
			{
				WindowDuration:      10 * time.Minute,
				MaxReadsInWindow:    int64(float64(c.ReadRate.ReadsPerSecond) * 600 * 0.6),
				MaxBytesInWindow:    int64(float64(c.ReadRate.BytesPerSecond) * 600 * 0.6),
				CooldownAfterWindow: 60 * time.Second,
			},
		}
		c.BehaviorConfig.EnablePeriodLimits = true
	default:
		c.PeriodLimits = nil
		c.BehaviorConfig.EnablePeriodLimits = false
	}
}

func (c CompleteThrottleConfig) Validate() error {
	if c.ReadRate.ReadsPerSecond < 0 {
		return fmt.Errorf("reads_per_second must be >= 0")
	}
	if c.ReadRate.ReadsPerSecond > 100000 {
		return fmt.Errorf("reads_per_second too high (%d), max 100000", c.ReadRate.ReadsPerSecond)
	}
	if c.ReadRate.BytesPerSecond < 0 {
		return fmt.Errorf("bytes_per_second must be >= 0")
	}
	if c.ReadRate.BytesPerSecond > 10*1024*1024*1024 {
		return fmt.Errorf("bytes_per_second too high (%d bytes), max 10GB/s", c.ReadRate.BytesPerSecond)
	}
	if c.TimeSlice.ScanDuration <= 0 {
		return fmt.Errorf("scan_slice duration must be > 0")
	}
	if c.TimeSlice.SleepDuration < 0 {
		return fmt.Errorf("sleep_slice duration must be >= 0")
	}
	if c.GlobalLimits.MaxConcurrentTasks <= 0 {
		return fmt.Errorf("max_concurrent_tasks must be > 0")
	}
	if c.GlobalLimits.ReserveForBusiness.MaxUtilizationPct < 0 ||
		c.GlobalLimits.ReserveForBusiness.MaxUtilizationPct > 1.0 {
		return fmt.Errorf("max_utilization_pct must be between 0 and 1.0")
	}
	if c.BehaviorConfig.WarmupStartPct < 0 || c.BehaviorConfig.WarmupStartPct > 1.0 {
		return fmt.Errorf("warmup_start_pct must be between 0 and 1.0")
	}
	if c.ReadRate.MinReadIntervalUs < 0 {
		return fmt.Errorf("min_read_interval_us must be >= 0")
	}
	if c.ReadRate.MaxConsecutiveReads < 0 {
		return fmt.Errorf("max_consecutive_reads must be >= 0")
	}
	return nil
}

func (c CompleteThrottleConfig) ToScanThrottleConfig() ThrottleConfig {
	priority := c.BehaviorConfig.Priority
	if c.Preset == ProfileUltraSafe || c.Preset == ProfileConservative {
		priority = PriorityLow
	}

	return ThrottleConfig{
		MaxIOPS:         int(c.ReadRate.ReadsPerSecond),
		MaxBandwidthBps: c.ReadRate.BytesPerSecond,
		ScanSlice:       c.TimeSlice.ScanDuration,
		SleepSlice:      c.TimeSlice.SleepDuration,
		Priority:        priority,
		EnableAdaptive:  c.BehaviorConfig.EnableAdaptive,
	}
}

func (c CompleteThrottleConfig) Description() string {
	var desc string
	switch c.Preset {
	case ProfileUltraSafe:
		desc = "Ultra-safe: ~30 IOPS, 5 MB/s, 17% duty cycle. For high-load production systems."
	case ProfileConservative:
		desc = "Conservative: ~80 IOPS, 15 MB/s, 30% duty cycle. Default for daytime."
	case ProfileBalanced:
		desc = "Balanced: ~200 IOPS, 50 MB/s, 67% duty cycle. Good for off-peak."
	case ProfileAggressive:
		desc = "Aggressive: ~500 IOPS, 150 MB/s, 89% duty cycle. Maintenance windows only."
	case ProfileNightOnly:
		desc = "Night-only: ~800 IOPS, 300 MB/s, 100% duty cycle. Midnight batch."
	default:
		desc = "Custom configuration"
	}
	return desc
}

func (c CompleteThrottleConfig) Summary() map[string]interface{} {
	dutyCycle := 100.0
	if total := c.TimeSlice.ScanDuration + c.TimeSlice.SleepDuration; total > 0 {
		dutyCycle = float64(c.TimeSlice.ScanDuration) / float64(total) * 100
	}
	return map[string]interface{}{
		"preset":               c.Preset,
		"description":          c.Description(),
		"max_iops":             c.ReadRate.ReadsPerSecond,
		"max_mb_per_sec":       float64(c.ReadRate.BytesPerSecond) / (1024 * 1024),
		"duty_cycle_pct":       fmt.Sprintf("%.1f%%", dutyCycle),
		"scan_ms":              c.TimeSlice.ScanDuration.Milliseconds(),
		"sleep_ms":             c.TimeSlice.SleepDuration.Milliseconds(),
		"priority":             c.BehaviorConfig.Priority,
		"adaptive_enabled":     c.BehaviorConfig.EnableAdaptive,
		"period_limits_count":  len(c.PeriodLimits),
		"max_utilization_pct":  fmt.Sprintf("%.0f%%", c.GlobalLimits.ReserveForBusiness.MaxUtilizationPct*100),
		"warmup_seconds":       float64(c.BehaviorConfig.StartupWarmupMs) / 1000,
	}
}

func ListPresets() []map[string]interface{} {
	presets := []PresetProfile{
		ProfileUltraSafe,
		ProfileConservative,
		ProfileBalanced,
		ProfileAggressive,
		ProfileNightOnly,
	}
	result := make([]map[string]interface{}, 0, len(presets))
	for _, p := range presets {
		cfg := GetPresetConfig(p)
		result = append(result, cfg.Summary())
	}
	return result
}

type WarmupController struct {
	config         BehaviorConfig
	startTime      time.Time
	elapsedMs      int64
	fullRPS        int64
	fullBPS        int64
	enabled        bool
}

func NewWarmupController(cfg BehaviorConfig, fullRPS, fullBPS int64) *WarmupController {
	return &WarmupController{
		config:    cfg,
		startTime: time.Now(),
		fullRPS:   fullRPS,
		fullBPS:   fullBPS,
		enabled:   cfg.StartupWarmupMs > 0,
	}
}

func (w *WarmupController) CurrentLimits() (rps, bps int64) {
	if !w.enabled || w.fullRPS <= 0 {
		return w.fullRPS, w.fullBPS
	}

	w.elapsedMs = time.Since(w.startTime).Milliseconds()
	if w.elapsedMs >= w.config.StartupWarmupMs {
		return w.fullRPS, w.fullBPS
	}

	progress := float64(w.elapsedMs) / float64(w.config.StartupWarmupMs)
	startFactor := w.config.WarmupStartPct
	factor := startFactor + (1.0-startFactor)*progress

	rps = int64(float64(w.fullRPS) * factor)
	bps = int64(float64(w.fullBPS) * factor)

	if rps < 1 {
		rps = 1
	}
	if bps < 4096 {
		bps = 4096
	}
	return rps, bps
}

func (w *WarmupController) IsWarmingUp() bool {
	if !w.enabled {
		return false
	}
	return time.Since(w.startTime).Milliseconds() < w.config.StartupWarmupMs
}

func (w *WarmupController) WarmupProgressPct() float64 {
	if !w.enabled || w.config.StartupWarmupMs <= 0 {
		return 100.0
	}
	elapsed := time.Since(w.startTime).Milliseconds()
	if elapsed >= w.config.StartupWarmupMs {
		return 100.0
	}
	return float64(elapsed) / float64(w.config.StartupWarmupMs) * 100
}
