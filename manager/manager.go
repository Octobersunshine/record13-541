package manager

import (
	"context"
	"disk-scan/scanner"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID         string                      `json:"id"`
	Name       string                      `json:"name"`
	CreatedAt  time.Time                   `json:"created_at"`
	Scanner    *scanner.DiskScanner        `json:"-"`
	Config     scanner.ScanConfig          `json:"config"`
	Observers  map[string]chan ProgressEvent `json:"-"`
	observerMu sync.RWMutex
}

type ProgressEvent struct {
	TaskID     string             `json:"task_id"`
	Current    int64              `json:"current"`
	Total      int64              `json:"total"`
	Percent    float64            `json:"percent"`
	BadBlocks  []scanner.BadBlock `json:"bad_blocks"`
	Final      bool               `json:"final"`
	Result     *scanner.ScanResult `json:"result,omitempty"`
	Throttled  bool               `json:"throttled"`
}

type TaskManager struct {
	tasks map[string]*Task
	mu    sync.RWMutex
}

var (
	instance *TaskManager
	once     sync.Once
)

func GetTaskManager() *TaskManager {
	once.Do(func() {
		instance = &TaskManager{
			tasks: make(map[string]*Task),
		}
	})
	return instance
}

func (tm *TaskManager) CreateTask(name string, config scanner.ScanConfig) (*Task, error) {
	taskID := uuid.New().String()

	ds := scanner.NewDiskScanner(config)
	ds.SetTaskID(taskID)

	task := &Task{
		ID:        taskID,
		Name:      name,
		CreatedAt: time.Now(),
		Config:    config,
		Scanner:   ds,
		Observers: make(map[string]chan ProgressEvent),
	}

	tm.mu.Lock()
	tm.tasks[taskID] = task
	tm.mu.Unlock()

	return task, nil
}

func (tm *TaskManager) StartTask(taskID string) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Scanner.IsRunning() {
		return fmt.Errorf("task %s is already running", taskID)
	}

	go func() {
		ctx := context.Background()

		callback := func(current, total int64, percent float64, badBlocks []scanner.BadBlock, throttled bool) {
			event := ProgressEvent{
				TaskID:    taskID,
				Current:   current,
				Total:     total,
				Percent:   percent,
				BadBlocks: badBlocks,
				Final:     false,
				Throttled: throttled,
			}
			task.broadcastEvent(event)
		}

		result, _ := task.Scanner.Start(ctx, callback)

		finalEvent := ProgressEvent{
			TaskID:    taskID,
			Current:   result.Scanned,
			Total:     result.TotalBlocks,
			Percent:   result.Percent,
			BadBlocks: result.BadBlocks,
			Final:     true,
			Result:    result,
			Throttled: false,
		}
		task.broadcastEvent(finalEvent)
	}()

	return nil
}

func (tm *TaskManager) CancelTask(taskID string) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Scanner.Cancel()
	return nil
}

func (tm *TaskManager) PauseTask(taskID string) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Scanner.Pause()
	return nil
}

func (tm *TaskManager) ResumeTask(taskID string) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Scanner.Resume()
	return nil
}

func (tm *TaskManager) PauseAllTasks() {
	tm.mu.RLock()
	tasks := make([]*Task, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		tasks = append(tasks, t)
	}
	tm.mu.RUnlock()

	for _, t := range tasks {
		t.Scanner.Pause()
	}
}

func (tm *TaskManager) ResumeAllTasks() {
	tm.mu.RLock()
	tasks := make([]*Task, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		tasks = append(tasks, t)
	}
	tm.mu.RUnlock()

	for _, t := range tasks {
		t.Scanner.Resume()
	}
}

func (tm *TaskManager) UpdateTaskRateLimits(taskID string, maxIOPS int, maxBandwidthBps int64) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Scanner.UpdateRateLimits(maxIOPS, maxBandwidthBps)
	return nil
}

func (tm *TaskManager) UpdateTaskTimeSlice(taskID string, scanSlice, sleepSlice time.Duration) error {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Scanner.UpdateTimeSlice(scanSlice, sleepSlice)
	return nil
}

func (tm *TaskManager) GetTask(taskID string) (*Task, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task %s not found", taskID)
	}
	return task, nil
}

func (tm *TaskManager) ListTasks() []*Task {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tasks := make([]*Task, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (tm *TaskManager) GetTaskProgress(taskID string) (*scanner.ScanResult, error) {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	return task.Scanner.GetResult(), nil
}

func (tm *TaskManager) Subscribe(taskID string) (string, <-chan ProgressEvent, error) {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return "", nil, fmt.Errorf("task %s not found", taskID)
	}

	observerID := uuid.New().String()
	ch := make(chan ProgressEvent, 100)

	task.observerMu.Lock()
	task.Observers[observerID] = ch
	task.observerMu.Unlock()

	return observerID, ch, nil
}

func (tm *TaskManager) Unsubscribe(taskID string, observerID string) {
	tm.mu.RLock()
	task, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return
	}

	task.observerMu.Lock()
	if ch, ok := task.Observers[observerID]; ok {
		close(ch)
		delete(task.Observers, observerID)
	}
	task.observerMu.Unlock()
}

func (t *Task) broadcastEvent(event ProgressEvent) {
	t.observerMu.RLock()
	observers := make([]chan ProgressEvent, 0, len(t.Observers))
	for _, ch := range t.Observers {
		observers = append(observers, ch)
	}
	t.observerMu.RUnlock()

	for _, ch := range observers {
		select {
		case ch <- event:
		default:
		}
	}
}

type GlobalThrottleStatus struct {
	ActiveTaskCount    int64            `json:"active_task_count"`
	MaxConcurrentTasks int64            `json:"max_concurrent_tasks"`
	CurrentMaxIOPS     int              `json:"current_max_iops"`
	CurrentMaxBWMBps   float64          `json:"current_max_bw_mbps"`
	SystemLoad         scanner.SystemLoad `json:"system_load"`
	EmergencyPaused    bool             `json:"emergency_paused"`
}

type TaskInfo struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	CreatedAt  time.Time           `json:"created_at"`
	Config     scanner.ScanConfig  `json:"config"`
	Progress   *scanner.ScanResult `json:"progress"`
	IsRunning  bool                `json:"is_running"`
	IsPaused   bool                `json:"is_paused"`
}

func (tm *TaskManager) GetTaskInfo(taskID string) (*TaskInfo, error) {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	return &TaskInfo{
		ID:        task.ID,
		Name:      task.Name,
		CreatedAt: task.CreatedAt,
		Config:    task.Config,
		Progress:  task.Scanner.GetResult(),
		IsRunning: task.Scanner.IsRunning(),
		IsPaused:  task.Scanner.IsPaused(),
	}, nil
}

func (tm *TaskManager) ListTaskInfos() []*TaskInfo {
	tasks := tm.ListTasks()
	infos := make([]*TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		infos = append(infos, &TaskInfo{
			ID:        task.ID,
			Name:      task.Name,
			CreatedAt: task.CreatedAt,
			Config:    task.Config,
			Progress:  task.Scanner.GetResult(),
			IsRunning: task.Scanner.IsRunning(),
			IsPaused:  task.Scanner.IsPaused(),
		})
	}
	return infos
}

func (tm *TaskManager) GetGlobalThrottleStatus() GlobalThrottleStatus {
	gt := scanner.GetGlobalIOThrottle()
	maxIOPS, maxBW := gt.GetCurrentGlobalLimits()
	return GlobalThrottleStatus{
		ActiveTaskCount:    gt.GetActiveCount(),
		MaxConcurrentTasks: 2,
		CurrentMaxIOPS:     maxIOPS,
		CurrentMaxBWMBps:   float64(maxBW) / (1024 * 1024),
		SystemLoad:         gt.GetSystemLoad(),
		EmergencyPaused:    gt.IsEmergencyPaused(),
	}
}

func (tm *TaskManager) EmergencyPause() {
	scanner.GetGlobalIOThrottle().EmergencyPause()
}

func (tm *TaskManager) EmergencyResume() {
	scanner.GetGlobalIOThrottle().EmergencyResume()
}

func (tm *TaskManager) SetGlobalLimits(maxConcurrent int64, maxTotalIOPS int64, maxTotalBW int64) {
	gt := scanner.GetGlobalIOThrottle()
	if maxConcurrent > 0 {
		gt.SetMaxConcurrent(maxConcurrent)
	}
	if maxTotalIOPS > 0 || maxTotalBW > 0 {
		gt.SetGlobalLimits(maxTotalIOPS, maxTotalBW)
	}
}

func (ti *TaskInfo) ToJSON() (string, error) {
	data, err := json.Marshal(ti)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
