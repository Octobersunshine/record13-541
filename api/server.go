package api

import (
	"disk-scan/manager"
	"disk-scan/scanner"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	addr    string
	manager *manager.TaskManager
}

type CreateTaskRequest struct {
	Name         string `json:"name"`
	DiskPath     string `json:"disk_path"`
	BlockSize    int64  `json:"block_size"`
	StartOffset  int64  `json:"start_offset"`
	EndOffset    int64  `json:"end_offset"`
	Speed        string `json:"speed"`
	AutoStart    bool   `json:"auto_start"`
	PresetName   string `json:"preset_name"`
	Throttle     *ThrottleRequest `json:"throttle,omitempty"`
	ReadLimit    *ReadLimitRequest `json:"read_limit,omitempty"`
}

type ThrottleRequest struct {
	MaxIOPS         int    `json:"max_iops"`
	MaxBandwidthMB  int    `json:"max_bandwidth_mb"`
	ScanSliceMs     int    `json:"scan_slice_ms"`
	SleepSliceMs    int    `json:"sleep_slice_ms"`
	Priority        int    `json:"priority"`
	EnableAdaptive  bool   `json:"enable_adaptive"`
}

type ReadLimitRequest struct {
	ReadsPerSecond        int64 `json:"reads_per_second"`
	MBPerSecond           int64 `json:"mb_per_second"`
	MinReadIntervalUs     int64 `json:"min_read_interval_us"`
	MaxConsecutiveReads   int   `json:"max_consecutive_reads"`
	AfterEachReadSleepUs  int64 `json:"after_each_read_sleep_us"`
	StartupWarmupSec      int64 `json:"startup_warmup_sec"`
	EnablePeriodLimits    bool  `json:"enable_period_limits"`
}

type UpdateRateLimitsRequest struct {
	MaxIOPS        int `json:"max_iops"`
	MaxBandwidthMB int `json:"max_bandwidth_mb"`
}

type UpdateTimeSliceRequest struct {
	ScanSliceMs  int `json:"scan_slice_ms"`
	SleepSliceMs int `json:"sleep_slice_ms"`
}

type GlobalLimitsRequest struct {
	MaxConcurrent   int64 `json:"max_concurrent"`
	MaxTotalIOPS    int64 `json:"max_total_iops"`
	MaxTotalBWMB    int64 `json:"max_total_bw_mb"`
}

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func NewServer(addr string) *Server {
	return &Server{
		addr:    addr,
		manager: manager.GetTaskManager(),
	}
}

func (s *Server) Run() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/tasks/", s.handleTaskByID)
	mux.HandleFunc("/api/stream/", s.handleStream)
	mux.HandleFunc("/api/throttle/global", s.handleGlobalThrottle)
	mux.HandleFunc("/api/throttle/global/pause", s.handleGlobalPause)
	mux.HandleFunc("/api/throttle/global/resume", s.handleGlobalResume)
	mux.HandleFunc("/api/throttle/all/pause", s.handleAllPause)
	mux.HandleFunc("/api/throttle/all/resume", s.handleAllResume)
	mux.HandleFunc("/api/throttle/presets", s.handlePresets)
	mux.HandleFunc("/health", s.handleHealth)

	srv := &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("Disk scan server starting on %s", s.addr)
	log.Printf("=== Task Management ===")
	log.Printf("  POST   /api/tasks                  - Create scan task (use preset_name or read_limit)")
	log.Printf("  GET    /api/tasks                  - List all tasks")
	log.Printf("  GET    /api/tasks/{id}             - Get task status and progress")
	log.Printf("  POST   /api/tasks/{id}/start       - Start a task")
	log.Printf("  POST   /api/tasks/{id}/cancel      - Cancel a task")
	log.Printf("  POST   /api/tasks/{id}/pause       - Pause a running task")
	log.Printf("  POST   /api/tasks/{id}/resume      - Resume a paused task")
	log.Printf("  POST   /api/tasks/{id}/rate        - Update task IO rate limits (IOPS/MB)")
	log.Printf("  POST   /api/tasks/{id}/timeslice   - Update task time-slice config")
	log.Printf("=== Global IO Control ===")
	log.Printf("  GET    /api/throttle/presets       - List all preset profiles (ultra_safe..night_only)")
	log.Printf("  GET    /api/throttle/global        - Get global throttle status")
	log.Printf("  PUT    /api/throttle/global        - Set global IO limits")
	log.Printf("  POST   /api/throttle/global/pause  - EMERGENCY: pause all IO (critical!)")
	log.Printf("  POST   /api/throttle/global/resume - Resume from emergency pause")
	log.Printf("  POST   /api/throttle/all/pause     - Pause all scan tasks")
	log.Printf("  POST   /api/throttle/all/resume    - Resume all paused tasks")
	log.Printf("=== Streaming ===")
	log.Printf("  GET    /api/stream/{id}            - SSE real-time progress stream")
	log.Printf("  GET    /health                     - Health check (includes IO status)")
	return srv.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	status := s.manager.GetGlobalThrottleStatus()
	writeSuccess(w, map[string]interface{}{
		"status":    "ok",
		"time":      time.Now().Format(time.RFC3339),
		"io_status": status,
	})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listTasks(w, r)
	case http.MethodPost:
		s.createTask(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "task id is required")
		return
	}

	taskID := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.getTask(w, r, taskID)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	if len(parts) == 2 {
		action := parts[1]
		switch action {
		case "start":
			s.requirePOST(w, r, func() { s.startTask(w, r, taskID) })
		case "cancel":
			s.requirePOST(w, r, func() { s.cancelTask(w, r, taskID) })
		case "pause":
			s.requirePOST(w, r, func() { s.pauseTask(w, r, taskID) })
		case "resume":
			s.requirePOST(w, r, func() { s.resumeTask(w, r, taskID) })
		case "rate":
			s.requirePOSTOrPUT(w, r, func() { s.updateTaskRate(w, r, taskID) })
		case "timeslice":
			s.requirePOSTOrPUT(w, r, func() { s.updateTaskTimeSlice(w, r, taskID) })
		default:
			writeError(w, http.StatusNotFound, "unknown action: "+action)
		}
		return
	}

	writeError(w, http.StatusNotFound, "invalid path")
}

func (s *Server) requirePOST(w http.ResponseWriter, r *http.Request, handler func()) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST method")
		return
	}
	handler()
}

func (s *Server) requirePOSTOrPUT(w http.ResponseWriter, r *http.Request, handler func()) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "use POST or PUT method")
		return
	}
	handler()
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Name == "" {
		req.Name = fmt.Sprintf("scan-%s", time.Now().Format("20060102-150405"))
	}

	var speed time.Duration
	if req.Speed != "" {
		var err error
		speed, err = time.ParseDuration(req.Speed)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid speed format: "+err.Error())
			return
		}
	}

	config := scanner.ScanConfig{
		DiskPath:    req.DiskPath,
		BlockSize:   req.BlockSize,
		StartOffset: req.StartOffset,
		EndOffset:   req.EndOffset,
		Speed:       speed,
	}
	fullThrottleInitialized := false

	if req.PresetName != "" {
		config.UsePreset = true
		config.PresetName = scanner.PresetProfile(req.PresetName)
		preset := scanner.GetPresetConfig(config.PresetName)
		config.FullThrottle = preset
		config.Throttle = preset.ToScanThrottleConfig()
		fullThrottleInitialized = true
	}

	if req.ReadLimit != nil {
		rl := req.ReadLimit
		if !fullThrottleInitialized {
			fullCfg := scanner.DefaultThrottleConfig()
			config.FullThrottle = fullCfg
			fullThrottleInitialized = true
		}

		if rl.ReadsPerSecond > 0 {
			config.FullThrottle.ReadRate.ReadsPerSecond = rl.ReadsPerSecond
			config.Throttle.MaxIOPS = int(rl.ReadsPerSecond)
		}
		if rl.MBPerSecond > 0 {
			config.FullThrottle.ReadRate.BytesPerSecond = rl.MBPerSecond * 1024 * 1024
			config.Throttle.MaxBandwidthBps = rl.MBPerSecond * 1024 * 1024
		}
		if rl.MinReadIntervalUs > 0 {
			config.FullThrottle.ReadRate.MinReadIntervalUs = rl.MinReadIntervalUs
		}
		if rl.MaxConsecutiveReads > 0 {
			config.FullThrottle.ReadRate.MaxConsecutiveReads = rl.MaxConsecutiveReads
		}
		if rl.AfterEachReadSleepUs > 0 {
			config.FullThrottle.ReadRate.AfterReadSleepUs = rl.AfterEachReadSleepUs
		}
		if rl.StartupWarmupSec > 0 {
			config.FullThrottle.BehaviorConfig.StartupWarmupMs = rl.StartupWarmupSec * 1000
		}
		config.FullThrottle.BehaviorConfig.EnablePeriodLimits = rl.EnablePeriodLimits

		if config.Throttle.MaxIOPS == 0 && config.FullThrottle.ReadRate.ReadsPerSecond > 0 {
			config.Throttle.MaxIOPS = int(config.FullThrottle.ReadRate.ReadsPerSecond)
		}
		if config.Throttle.MaxBandwidthBps == 0 && config.FullThrottle.ReadRate.BytesPerSecond > 0 {
			config.Throttle.MaxBandwidthBps = config.FullThrottle.ReadRate.BytesPerSecond
		}
		config.Throttle = config.FullThrottle.ToScanThrottleConfig()
	}

	if req.Throttle != nil {
		t := req.Throttle
		config.Throttle = scanner.ThrottleConfig{
			MaxIOPS:         t.MaxIOPS,
			MaxBandwidthBps: int64(t.MaxBandwidthMB) * 1024 * 1024,
			ScanSlice:       time.Duration(t.ScanSliceMs) * time.Millisecond,
			SleepSlice:      time.Duration(t.SleepSliceMs) * time.Millisecond,
			Priority:        scanner.IOPriority(t.Priority),
			EnableAdaptive:  t.EnableAdaptive,
		}
		if !fullThrottleInitialized {
			config.FullThrottle = scanner.DefaultThrottleConfig()
			fullThrottleInitialized = true
		}
		if t.MaxIOPS > 0 {
			config.FullThrottle.ReadRate.ReadsPerSecond = int64(t.MaxIOPS)
		}
		if t.MaxBandwidthMB > 0 {
			config.FullThrottle.ReadRate.BytesPerSecond = int64(t.MaxBandwidthMB) * 1024 * 1024
		}
	}

	if err := config.FullThrottle.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "throttle config invalid: "+err.Error())
		return
	}

	config.FullThrottle.OverrideDefaults = true

	task, err := s.manager.CreateTask(req.Name, config)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create task: "+err.Error())
		return
	}

	if req.AutoStart {
		if err := s.manager.StartTask(task.ID); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to start task: "+err.Error())
			return
		}
	}

	info, _ := s.manager.GetTaskInfo(task.ID)
	writeSuccess(w, info)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	infos := s.manager.ListTaskInfos()
	writeSuccess(w, infos)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request, taskID string) {
	info, err := s.manager.GetTaskInfo(taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeSuccess(w, info)
}

func (s *Server) startTask(w http.ResponseWriter, r *http.Request, taskID string) {
	if err := s.manager.StartTask(taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, info)
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request, taskID string) {
	if err := s.manager.CancelTask(taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, info)
}

func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request, taskID string) {
	if err := s.manager.PauseTask(taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, info)
}

func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request, taskID string) {
	if err := s.manager.ResumeTask(taskID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, info)
}

func (s *Server) updateTaskRate(w http.ResponseWriter, r *http.Request, taskID string) {
	var req UpdateRateLimitsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.MaxIOPS <= 0 && req.MaxBandwidthMB <= 0 {
		writeError(w, http.StatusBadRequest, "at least one of max_iops or max_bandwidth_mb must be positive")
		return
	}

	maxBW := int64(req.MaxBandwidthMB) * 1024 * 1024
	if err := s.manager.UpdateTaskRateLimits(taskID, req.MaxIOPS, maxBW); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, map[string]interface{}{
		"message":       fmt.Sprintf("rate limits updated: IOPS=%d, BW=%d MB/s", req.MaxIOPS, req.MaxBandwidthMB),
		"effective_max": map[string]interface{}{
			"max_iops":       req.MaxIOPS,
			"max_bandwidth":  req.MaxBandwidthMB,
			"unit":           "MB/s",
		},
		"task": info,
	})
}

func (s *Server) updateTaskTimeSlice(w http.ResponseWriter, r *http.Request, taskID string) {
	var req UpdateTimeSliceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.ScanSliceMs <= 0 || req.SleepSliceMs < 0 {
		writeError(w, http.StatusBadRequest, "scan_slice_ms must be > 0, sleep_slice_ms must be >= 0")
		return
	}

	scanSlice := time.Duration(req.ScanSliceMs) * time.Millisecond
	sleepSlice := time.Duration(req.SleepSliceMs) * time.Millisecond

	if err := s.manager.UpdateTaskTimeSlice(taskID, scanSlice, sleepSlice); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	info, _ := s.manager.GetTaskInfo(taskID)
	writeSuccess(w, map[string]interface{}{
		"message": fmt.Sprintf("time-slice updated: scan=%dms, sleep=%dms, duty=%.0f%%",
			req.ScanSliceMs, req.SleepSliceMs,
			float64(req.ScanSliceMs)/float64(req.ScanSliceMs+req.SleepSliceMs)*100),
		"task": info,
	})
}

func (s *Server) handleGlobalThrottle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		status := s.manager.GetGlobalThrottleStatus()
		writeSuccess(w, status)
	case http.MethodPut, http.MethodPost:
		var req GlobalLimitsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}
		maxBW := req.MaxTotalBWMB * 1024 * 1024
		s.manager.SetGlobalLimits(req.MaxConcurrent, req.MaxTotalIOPS, maxBW)
		status := s.manager.GetGlobalThrottleStatus()
		writeSuccess(w, map[string]interface{}{
			"message": "global limits updated",
			"status":  status,
		})
	default:
		writeError(w, http.StatusMethodNotAllowed, "use GET or PUT method")
	}
}

func (s *Server) handleGlobalPause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST for emergency pause")
		return
	}
	s.manager.EmergencyPause()
	status := s.manager.GetGlobalThrottleStatus()
	log.Printf("⚠️  EMERGENCY PAUSE triggered! All IO throttled to minimum.")
	writeSuccess(w, map[string]interface{}{
		"warning": "EMERGENCY PAUSE ACTIVE - All scan tasks are throttled to near-zero IO",
		"action":  "Call POST /api/throttle/global/resume to restore normal operation",
		"status":  status,
	})
}

func (s *Server) handleGlobalResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST for resume")
		return
	}
	s.manager.EmergencyResume()
	status := s.manager.GetGlobalThrottleStatus()
	log.Printf("Emergency pause released. IO limits restored.")
	writeSuccess(w, map[string]interface{}{
		"message": "Emergency pause released",
		"status":  status,
	})
}

func (s *Server) handleAllPause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	s.manager.PauseAllTasks()
	writeSuccess(w, map[string]string{
		"message": "all tasks paused",
	})
}

func (s *Server) handleAllResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	s.manager.ResumeAllTasks()
	writeSuccess(w, map[string]string{
		"message": "all tasks resumed",
	})
}

func (s *Server) handlePresets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "use GET to list presets")
		return
	}
	presets := scanner.ListPresets()
	writeSuccess(w, map[string]interface{}{
		"count":            len(presets),
		"presets":          presets,
		"usage_hint":       "Set preset_name when creating task, e.g. \"preset_name\": \"conservative\"",
		"recommended_for": map[string]string{
			"ultra_safe":   "Production peak hours - minimum impact",
			"conservative": "Daytime business hours - default",
			"balanced":     "Off-peak hours - good balance",
			"aggressive":   "Maintenance windows - fast scan",
			"night_only":   "Midnight batch - maximum speed",
		},
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "use GET for SSE stream")
		return
	}

	taskID := strings.TrimPrefix(r.URL.Path, "/api/stream/")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task id is required")
		return
	}

	_, err := s.manager.GetTask(taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	observerID, eventChan, err := s.manager.Subscribe(taskID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer s.manager.Unsubscribe(taskID, observerID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	notify := r.Context().Done()

	globalStatus := s.manager.GetGlobalThrottleStatus()
	writeSSE(w, flusher, "connected", map[string]interface{}{
		"task_id":       taskID,
		"message":       "subscribed to progress stream",
		"global_status": globalStatus,
	})

	progress, _ := s.manager.GetTaskProgress(taskID)
	if progress.Status != scanner.StatusPending {
		writeSSE(w, flusher, "progress", map[string]interface{}{
			"task_id":    taskID,
			"current":    progress.Scanned,
			"total":      progress.TotalBlocks,
			"percent":    formatPercent(progress.Percent),
			"status":     progress.Status,
			"bad_blocks": len(progress.BadBlocks),
			"throttled":  progress.IsThrottled,
			"rate_stats": progress.RateStats,
		})
	}

	for {
		select {
		case <-notify:
			log.Printf("Client disconnected from stream for task %s", taskID)
			return
		case event, ok := <-eventChan:
			if !ok {
				return
			}

			eventData := map[string]interface{}{
				"task_id":    event.TaskID,
				"current":    event.Current,
				"total":      event.Total,
				"percent":    formatPercent(event.Percent),
				"bad_blocks": len(event.BadBlocks),
				"final":      event.Final,
				"throttled":  event.Throttled,
			}

			if event.Final && event.Result != nil {
				eventData["status"] = event.Result.Status
				eventData["result"] = event.Result
				eventData["elapsed"] = event.Result.Elapsed
				eventData["rate_stats"] = event.Result.RateStats
			}

			eventType := "progress"
			if event.Final {
				eventType = "complete"
			}

			writeSSE(w, flusher, eventType, eventData)

			if event.Final {
				writeSSE(w, flusher, "done", map[string]string{
					"message": "scan completed",
				})
				return
			}
		}
	}
}

func writeSSE(w http.ResponseWriter, flusher http.Flusher, event string, data interface{}) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal SSE data: %v", err)
		return
	}

	fmt.Fprintf(w, "event: %s\n", event)
	fmt.Fprintf(w, "data: %s\n\n", string(jsonData))
	flusher.Flush()
}

func writeSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(Response{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(Response{
		Code:    code,
		Message: message,
	})
}

func formatPercent(p float64) string {
	return strconv.FormatFloat(p, 'f', 2, 64) + "%"
}
