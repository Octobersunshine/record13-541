package scanner

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type SystemLoad struct {
	IOUtilization float64
	IOQueueDepth  float64
	ReadLatencyMs float64
	WriteLatencyMs float64
	Timestamp     time.Time
}

type LoadMonitor struct {
	mu              sync.Mutex
	currentLoad     SystemLoad
	history         []SystemLoad
	historySize     int
	samplingInterval time.Duration
	stopChan        chan struct{}
	running         bool
	loadCallback    func(load SystemLoad)
	overrideLoad    *float64
}

func NewLoadMonitor(samplingInterval time.Duration) *LoadMonitor {
	return &LoadMonitor{
		historySize:      60,
		samplingInterval: samplingInterval,
		stopChan:         make(chan struct{}),
		history:          make([]SystemLoad, 0, 60),
	}
}

func (lm *LoadMonitor) Start(ctx context.Context) {
	lm.mu.Lock()
	if lm.running {
		lm.mu.Unlock()
		return
	}
	lm.running = true
	lm.mu.Unlock()

	go func() {
		ticker := time.NewTicker(lm.samplingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				lm.stop()
				return
			case <-lm.stopChan:
				return
			case <-ticker.C:
				load := lm.sampleLoad()
				lm.updateLoad(load)
				if lm.loadCallback != nil {
					lm.loadCallback(load)
				}
			}
		}
	}()
}

func (lm *LoadMonitor) Stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.running {
		close(lm.stopChan)
		lm.running = false
	}
}

func (lm *LoadMonitor) stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.running = false
}

func (lm *LoadMonitor) sampleLoad() SystemLoad {
	lm.mu.Lock()
	override := lm.overrideLoad
	lm.mu.Unlock()

	load := SystemLoad{
		Timestamp:      time.Now(),
		IOUtilization:  0.15 + float64(time.Now().UnixNano()%20)/100.0,
		IOQueueDepth:   1.0 + float64(time.Now().UnixNano()%5),
		ReadLatencyMs:  2.0 + float64(time.Now().UnixNano()%10)/2.0,
		WriteLatencyMs: 3.0 + float64(time.Now().UnixNano()%15)/3.0,
	}

	if override != nil {
		load.IOUtilization = *override
	}

	return load
}

func (lm *LoadMonitor) updateLoad(load SystemLoad) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.currentLoad = load
	lm.history = append(lm.history, load)
	if len(lm.history) > lm.historySize {
		lm.history = lm.history[len(lm.history)-lm.historySize:]
	}
}

func (lm *LoadMonitor) GetCurrentLoad() SystemLoad {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.currentLoad
}

func (lm *LoadMonitor) GetAverageLoad(duration time.Duration) SystemLoad {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if len(lm.history) == 0 {
		return lm.currentLoad
	}

	cutoff := time.Now().Add(-duration)
	var samples []SystemLoad
	for i := len(lm.history) - 1; i >= 0; i-- {
		if lm.history[i].Timestamp.After(cutoff) {
			samples = append(samples, lm.history[i])
		} else {
			break
		}
	}

	if len(samples) == 0 {
		return lm.currentLoad
	}

	var avg SystemLoad
	avg.Timestamp = time.Now()
	for _, s := range samples {
		avg.IOUtilization += s.IOUtilization
		avg.IOQueueDepth += s.IOQueueDepth
		avg.ReadLatencyMs += s.ReadLatencyMs
		avg.WriteLatencyMs += s.WriteLatencyMs
	}
	n := float64(len(samples))
	avg.IOUtilization /= n
	avg.IOQueueDepth /= n
	avg.ReadLatencyMs /= n
	avg.WriteLatencyMs /= n

	return avg
}

func (lm *LoadMonitor) SetOverrideLoad(utilization float64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if utilization < 0 {
		lm.overrideLoad = nil
	} else {
		lm.overrideLoad = &utilization
	}
}

func (lm *LoadMonitor) OnLoadUpdate(callback func(load SystemLoad)) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.loadCallback = callback
}

type GlobalIOThrottle struct {
	activeScans     int64
	maxConcurrent   int64
	maxTotalIOPS    int64
	maxTotalBW      int64
	allocatedIOPS   map[string]int
	allocatedBW     map[string]int64
	mu              sync.Mutex
	loadMonitor     *LoadMonitor
	adaptiveCtrl    *AdaptiveController
	emergencyPause  int32
}

var (
	globalThrottleInstance *GlobalIOThrottle
	globalThrottleOnce     sync.Once
)

func GetGlobalIOThrottle() *GlobalIOThrottle {
	globalThrottleOnce.Do(func() {
		instance := &GlobalIOThrottle{
			maxConcurrent: 2,
			maxTotalIOPS:  500,
			maxTotalBW:    100 * 1024 * 1024,
			allocatedIOPS: make(map[string]int),
			allocatedBW:   make(map[string]int64),
			loadMonitor:   NewLoadMonitor(5 * time.Second),
		}
		instance.adaptiveCtrl = NewAdaptiveController(
			50, int(instance.maxTotalIOPS),
			5*1024*1024, instance.maxTotalBW,
		)

		ctx := context.Background()
		instance.loadMonitor.Start(ctx)
		instance.loadMonitor.OnLoadUpdate(func(load SystemLoad) {
			instance.adaptiveCtrl.ReportLoad(load.IOUtilization)
		})

		globalThrottleInstance = instance
	})
	return globalThrottleInstance
}

func (gt *GlobalIOThrottle) RegisterTask(taskID string, requestedIOPS int, requestedBW int64) (actualIOPS int, actualBW int64) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	if atomic.LoadInt32(&gt.emergencyPause) == 1 {
		return 1, 4096
	}

	active := atomic.LoadInt64(&gt.activeScans)
	if active >= gt.maxConcurrent {
		return 10, 64 * 1024
	}

	maxIOPS, maxBW := gt.adaptiveCtrl.GetCurrentLimits()

	totalAllocIOPS := 0
	for _, v := range gt.allocatedIOPS {
		totalAllocIOPS += v
	}
	totalAllocBW := int64(0)
	for _, v := range gt.allocatedBW {
		totalAllocBW += v
	}

	availableIOPS := maxIOPS - totalAllocIOPS
	availableBW := maxBW - totalAllocBW

	if availableIOPS < 0 {
		availableIOPS = 0
	}
	if availableBW < 0 {
		availableBW = 0
	}

	actualIOPS = requestedIOPS
	if actualIOPS > availableIOPS {
		actualIOPS = availableIOPS
	}
	if actualIOPS < 5 {
		actualIOPS = 5
	}

	actualBW = requestedBW
	if actualBW > availableBW {
		actualBW = availableBW
	}
	if actualBW < 64*1024 {
		actualBW = 64 * 1024
	}

	gt.allocatedIOPS[taskID] = actualIOPS
	gt.allocatedBW[taskID] = actualBW

	return actualIOPS, actualBW
}

func (gt *GlobalIOThrottle) UnregisterTask(taskID string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	delete(gt.allocatedIOPS, taskID)
	delete(gt.allocatedBW, taskID)
}

func (gt *GlobalIOThrottle) IncrementActive() int64 {
	return atomic.AddInt64(&gt.activeScans, 1)
}

func (gt *GlobalIOThrottle) DecrementActive() int64 {
	return atomic.AddInt64(&gt.activeScans, -1)
}

func (gt *GlobalIOThrottle) GetActiveCount() int64 {
	return atomic.LoadInt64(&gt.activeScans)
}

func (gt *GlobalIOThrottle) EmergencyPause() {
	atomic.StoreInt32(&gt.emergencyPause, 1)
	gt.adaptiveCtrl.Pause()
}

func (gt *GlobalIOThrottle) EmergencyResume() {
	atomic.StoreInt32(&gt.emergencyPause, 0)
	gt.adaptiveCtrl.Resume()
}

func (gt *GlobalIOThrottle) IsEmergencyPaused() bool {
	return atomic.LoadInt32(&gt.emergencyPause) == 1
}

func (gt *GlobalIOThrottle) SetMaxConcurrent(max int64) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	if max > 0 {
		gt.maxConcurrent = max
	}
}

func (gt *GlobalIOThrottle) SetGlobalLimits(maxTotalIOPS int64, maxTotalBW int64) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	if maxTotalIOPS > 0 {
		gt.maxTotalIOPS = maxTotalIOPS
	}
	if maxTotalBW > 0 {
		gt.maxTotalBW = maxTotalBW
	}
}

func (gt *GlobalIOThrottle) GetCurrentGlobalLimits() (maxIOPS int, maxBW int64) {
	return gt.adaptiveCtrl.GetCurrentLimits()
}

func (gt *GlobalIOThrottle) GetSystemLoad() SystemLoad {
	return gt.loadMonitor.GetAverageLoad(15 * time.Second)
}

func (gt *GlobalIOThrottle) ReallocateRates() map[string][2]interface{} {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	maxIOPS, maxBW := gt.adaptiveCtrl.GetCurrentLimits()
	taskCount := len(gt.allocatedIOPS)
	if taskCount == 0 {
		return nil
	}

	equalIOPS := maxIOPS / taskCount
	if equalIOPS < 5 {
		equalIOPS = 5
	}
	equalBW := maxBW / int64(taskCount)
	if equalBW < 64*1024 {
		equalBW = 64 * 1024
	}

	result := make(map[string][2]interface{})
	for taskID := range gt.allocatedIOPS {
		gt.allocatedIOPS[taskID] = equalIOPS
		gt.allocatedBW[taskID] = equalBW
		result[taskID] = [2]interface{}{equalIOPS, equalBW}
	}
	return result
}
