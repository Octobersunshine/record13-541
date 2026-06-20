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
	Name        string `json:"name"`
	DiskPath    string `json:"disk_path"`
	BlockSize   int64  `json:"block_size"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Speed       string `json:"speed"`
	AutoStart   bool   `json:"auto_start"`
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
	mux.HandleFunc("/health", s.handleHealth)

	srv := &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("Disk scan server starting on %s", s.addr)
	log.Printf("API Endpoints:")
	log.Printf("  POST   /api/tasks          - Create and optionally start a scan task")
	log.Printf("  GET    /api/tasks          - List all tasks")
	log.Printf("  GET    /api/tasks/{id}     - Get task status and progress")
	log.Printf("  POST   /api/tasks/{id}/start   - Start a task")
	log.Printf("  POST   /api/tasks/{id}/cancel  - Cancel a running task")
	log.Printf("  GET    /api/stream/{id}    - SSE real-time progress stream")
	log.Printf("  GET    /health             - Health check")
	return srv.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeSuccess(w, map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
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
			if r.Method == http.MethodPost {
				s.startTask(w, r, taskID)
			} else {
				writeError(w, http.StatusMethodNotAllowed, "use POST to start task")
			}
		case "cancel":
			if r.Method == http.MethodPost {
				s.cancelTask(w, r, taskID)
			} else {
				writeError(w, http.StatusMethodNotAllowed, "use POST to cancel task")
			}
		default:
			writeError(w, http.StatusNotFound, "unknown action: "+action)
		}
		return
	}

	writeError(w, http.StatusNotFound, "invalid path")
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

	writeSSE(w, flusher, "connected", map[string]string{
		"task_id": taskID,
		"message": "subscribed to progress stream",
	})

	progress, _ := s.manager.GetTaskProgress(taskID)
	if progress.Status != scanner.StatusPending {
		writeSSE(w, flusher, "progress", map[string]interface{}{
			"task_id":    taskID,
			"current":    progress.Scanned,
			"total":      progress.TotalBlocks,
			"percent":    progress.Percent,
			"status":     progress.Status,
			"bad_blocks": len(progress.BadBlocks),
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
			}

			if event.Final && event.Result != nil {
				eventData["status"] = event.Result.Status
				eventData["result"] = event.Result
				eventData["elapsed"] = event.Result.Elapsed
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
