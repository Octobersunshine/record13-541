package scanner

import (
	"context"
	"sync"
	"time"
)

type RateLimiter struct {
	mu              sync.Mutex
	maxIOPS         int
	maxBandwidthBps int64
	tokensIOPS      float64
	tokensBytes     float64
	lastRefill      time.Time
	enabled         bool
	burstFactor     float64
}

func NewRateLimiter(maxIOPS int, maxBandwidthBps int64) *RateLimiter {
	rl := &RateLimiter{
		maxIOPS:         maxIOPS,
		maxBandwidthBps: maxBandwidthBps,
		enabled:         maxIOPS > 0 || maxBandwidthBps > 0,
		burstFactor:     2.0,
		lastRefill:      time.Now(),
	}
	if rl.enabled {
		if maxIOPS > 0 {
			rl.tokensIOPS = float64(maxIOPS) * rl.burstFactor
		}
		if maxBandwidthBps > 0 {
			rl.tokensBytes = float64(maxBandwidthBps) * rl.burstFactor
		}
	}
	return rl
}

func (rl *RateLimiter) Wait(ctx context.Context, bytes int64) error {
	if !rl.enabled {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rl.mu.Lock()
		rl.refill()

		canIOPS := rl.maxIOPS <= 0 || rl.tokensIOPS >= 1.0
		canBytes := rl.maxBandwidthBps <= 0 || rl.tokensBytes >= float64(bytes)

		if canIOPS && canBytes {
			if rl.maxIOPS > 0 {
				rl.tokensIOPS -= 1.0
			}
			if rl.maxBandwidthBps > 0 {
				rl.tokensBytes -= float64(bytes)
			}
			rl.mu.Unlock()
			return nil
		}

		var waitTime time.Duration
		iopsWait := time.Duration(0)
		bytesWait := time.Duration(0)

		if rl.maxIOPS > 0 && rl.tokensIOPS < 1.0 {
			needed := 1.0 - rl.tokensIOPS
			iopsWait = time.Duration(float64(time.Second) * needed / float64(rl.maxIOPS))
		}
		if rl.maxBandwidthBps > 0 && rl.tokensBytes < float64(bytes) {
			needed := float64(bytes) - rl.tokensBytes
			bytesWait = time.Duration(float64(time.Second) * needed / float64(rl.maxBandwidthBps))
		}

		waitTime = iopsWait
		if bytesWait > waitTime {
			waitTime = bytesWait
		}
		if waitTime < 1*time.Millisecond {
			waitTime = 1 * time.Millisecond
		}
		rl.mu.Unlock()

		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (rl *RateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}

	if rl.maxIOPS > 0 {
		rl.tokensIOPS += float64(rl.maxIOPS) * elapsed
		maxTokens := float64(rl.maxIOPS) * rl.burstFactor
		if rl.tokensIOPS > maxTokens {
			rl.tokensIOPS = maxTokens
		}
	}

	if rl.maxBandwidthBps > 0 {
		rl.tokensBytes += float64(rl.maxBandwidthBps) * elapsed
		maxTokens := float64(rl.maxBandwidthBps) * rl.burstFactor
		if rl.tokensBytes > maxTokens {
			rl.tokensBytes = maxTokens
		}
	}

	rl.lastRefill = now
}

func (rl *RateLimiter) SetLimits(maxIOPS int, maxBandwidthBps int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.maxIOPS = maxIOPS
	rl.maxBandwidthBps = maxBandwidthBps
	rl.enabled = maxIOPS > 0 || maxBandwidthBps > 0
	rl.lastRefill = time.Now()

	if rl.enabled {
		if maxIOPS > 0 && rl.tokensIOPS == 0 {
			rl.tokensIOPS = float64(maxIOPS) * rl.burstFactor
		}
		if maxBandwidthBps > 0 && rl.tokensBytes == 0 {
			rl.tokensBytes = float64(maxBandwidthBps) * rl.burstFactor
		}
	}
}

func (rl *RateLimiter) GetLimits() (maxIOPS int, maxBandwidthBps int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.maxIOPS, rl.maxBandwidthBps
}

func (rl *RateLimiter) IsEnabled() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.enabled
}

type TimeSliceScheduler struct {
	scanDuration time.Duration
	sleepDuration time.Duration
	sliceTicker  *time.Ticker
	sleepEnd     chan struct{}
	mu           sync.Mutex
	enabled      bool
}

func NewTimeSliceScheduler(scanDuration, sleepDuration time.Duration) *TimeSliceScheduler {
	tss := &TimeSliceScheduler{
		scanDuration:  scanDuration,
		sleepDuration: sleepDuration,
		enabled:       scanDuration > 0 && sleepDuration > 0,
		sleepEnd:      make(chan struct{}),
	}
	return tss
}

func (tss *TimeSliceScheduler) Start(ctx context.Context) {
	if !tss.enabled {
		return
	}

	go func() {
		cycle := tss.scanDuration + tss.sleepDuration
		if cycle <= 0 {
			return
		}

		tss.mu.Lock()
		tss.sliceTicker = time.NewTicker(cycle)
		tss.mu.Unlock()

		defer func() {
			tss.mu.Lock()
			if tss.sliceTicker != nil {
				tss.sliceTicker.Stop()
			}
			tss.mu.Unlock()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tss.sliceTicker.C:
				select {
				case tss.sleepEnd <- struct{}{}:
				default:
				}

				sleepTimer := time.NewTimer(tss.sleepDuration)
				select {
				case <-ctx.Done():
					sleepTimer.Stop()
					return
				case <-sleepTimer.C:
				}
			}
		}
	}()
}

func (tss *TimeSliceScheduler) WaitIfSleeping(ctx context.Context) error {
	if !tss.enabled {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tss.sleepEnd:
		return nil
	default:
		return nil
	}
}

func (tss *TimeSliceScheduler) SetDurations(scanDuration, sleepDuration time.Duration) {
	tss.mu.Lock()
	defer tss.mu.Unlock()

	tss.scanDuration = scanDuration
	tss.sleepDuration = sleepDuration
	tss.enabled = scanDuration > 0 && sleepDuration > 0

	if tss.sliceTicker != nil {
		tss.sliceTicker.Stop()
		tss.sliceTicker = nil
	}
}

func (tss *TimeSliceScheduler) IsEnabled() bool {
	tss.mu.Lock()
	defer tss.mu.Unlock()
	return tss.enabled
}

type AdaptiveController struct {
	mu                    sync.Mutex
	currentIOPS           int
	currentBandwidth      int64
	minIOPS               int
	minBandwidth          int64
	maxIOPS               int
	maxBandwidth          int64
	loadThresholdHigh     float64
	loadThresholdLow      float64
	stepFactor            float64
	paused                bool
	pauseChan             chan struct{}
	resumeChan            chan struct{}
}

func NewAdaptiveController(minIOPS, maxIOPS int, minBandwidth, maxBandwidth int64) *AdaptiveController {
	return &AdaptiveController{
		currentIOPS:        maxIOPS,
		currentBandwidth:   maxBandwidth,
		minIOPS:            minIOPS,
		minBandwidth:       minBandwidth,
		maxIOPS:            maxIOPS,
		maxBandwidth:       maxBandwidth,
		loadThresholdHigh:  0.7,
		loadThresholdLow:   0.3,
		stepFactor:         0.25,
		pauseChan:          make(chan struct{}),
		resumeChan:         make(chan struct{}),
	}
}

func (ac *AdaptiveController) ReportLoad(loadLevel float64) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.paused {
		return
	}

	if loadLevel >= ac.loadThresholdHigh {
		ac.currentIOPS = int(float64(ac.currentIOPS) * (1.0 - ac.stepFactor))
		ac.currentBandwidth = int64(float64(ac.currentBandwidth) * (1.0 - ac.stepFactor))

		if ac.currentIOPS < ac.minIOPS {
			ac.currentIOPS = ac.minIOPS
		}
		if ac.currentBandwidth < ac.minBandwidth {
			ac.currentBandwidth = ac.minBandwidth
		}
	} else if loadLevel <= ac.loadThresholdLow {
		ac.currentIOPS = int(float64(ac.currentIOPS) * (1.0 + ac.stepFactor))
		ac.currentBandwidth = int64(float64(ac.currentBandwidth) * (1.0 + ac.stepFactor))

		if ac.currentIOPS > ac.maxIOPS {
			ac.currentIOPS = ac.maxIOPS
		}
		if ac.currentBandwidth > ac.maxBandwidth {
			ac.currentBandwidth = ac.maxBandwidth
		}
	}
}

func (ac *AdaptiveController) GetCurrentLimits() (iops int, bandwidth int64) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.currentIOPS, ac.currentBandwidth
}

func (ac *AdaptiveController) Pause() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if !ac.paused {
		ac.paused = true
		ac.currentIOPS = 0
		ac.currentBandwidth = 0
	}
}

func (ac *AdaptiveController) Resume() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.paused {
		ac.paused = false
		ac.currentIOPS = ac.maxIOPS
		ac.currentBandwidth = ac.maxBandwidth
	}
}

func (ac *AdaptiveController) IsPaused() bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.paused
}

func (ac *AdaptiveController) SetThresholds(high, low float64) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if high > 0 && high <= 1.0 {
		ac.loadThresholdHigh = high
	}
	if low > 0 && low <= 1.0 && low < ac.loadThresholdHigh {
		ac.loadThresholdLow = low
	}
}
