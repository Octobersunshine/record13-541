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
)

type BadBlock struct {
	Sector   int64  `json:"sector"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Reason   string `json:"reason"`
}

type ProgressCallback func(current, total int64, percent float64, badBlocks []BadBlock)

type ScanConfig struct {
	DiskPath    string
	BlockSize   int64
	StartOffset int64
	EndOffset   int64
	Speed       time.Duration
}

type ScanResult struct {
	Status      ScanStatus `json:"status"`
	TotalBlocks int64      `json:"total_blocks"`
	Scanned     int64      `json:"scanned"`
	Percent     float64    `json:"percent"`
	BadBlocks   []BadBlock `json:"bad_blocks"`
	Error       string     `json:"error,omitempty"`
	StartTime   time.Time  `json:"start_time"`
	EndTime     time.Time  `json:"end_time,omitempty"`
	Elapsed     string     `json:"elapsed"`
}

type DiskScanner struct {
	config  ScanConfig
	result  *ScanResult
	running bool
	cancel  context.CancelFunc
}

func NewDiskScanner(config ScanConfig) *DiskScanner {
	if config.BlockSize == 0 {
		config.BlockSize = 4096
	}
	if config.EndOffset == 0 {
		config.EndOffset = 1024 * 1024 * 1024
	}
	if config.Speed == 0 {
		config.Speed = 5 * time.Millisecond
	}
	return &DiskScanner{
		config: config,
		result: &ScanResult{
			Status:    StatusPending,
			BadBlocks: make([]BadBlock, 0),
		},
	}
}

func (ds *DiskScanner) Start(ctx context.Context, callback ProgressCallback) (*ScanResult, error) {
	if ds.running {
		return nil, fmt.Errorf("scanner is already running")
	}

	scanCtx, cancel := context.WithCancel(ctx)
	ds.cancel = cancel
	ds.running = true

	ds.result.Status = StatusRunning
	ds.result.StartTime = time.Now()
	ds.result.TotalBlocks = (ds.config.EndOffset - ds.config.StartOffset) / ds.config.BlockSize
	if ds.result.TotalBlocks <= 0 {
		ds.result.TotalBlocks = 1
	}

	defer func() {
		ds.running = false
		ds.result.EndTime = time.Now()
		elapsed := ds.result.EndTime.Sub(ds.result.StartTime)
		ds.result.Elapsed = formatDuration(elapsed)
	}()

	if err := ds.checkDiskAvailable(); err != nil {
		ds.result.Status = StatusFailed
		ds.result.Error = err.Error()
		return ds.result, err
	}

	for i := int64(0); i < ds.result.TotalBlocks; i++ {
		select {
		case <-scanCtx.Done():
			ds.result.Status = StatusCanceled
			ds.result.Error = "scan canceled by user"
			return ds.result, nil
		default:
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

		if callback != nil {
			callback(ds.result.Scanned, ds.result.TotalBlocks, ds.result.Percent, ds.result.BadBlocks)
		}

		if ds.config.Speed > 0 {
			sleepWithCtx(scanCtx, ds.config.Speed)
		}
	}

	ds.result.Status = StatusCompleted
	ds.result.Percent = 100.0
	return ds.result, nil
}

func (ds *DiskScanner) Cancel() {
	if ds.cancel != nil {
		ds.cancel()
	}
}

func (ds *DiskScanner) GetResult() *ScanResult {
	return ds.result
}

func (ds *DiskScanner) IsRunning() bool {
	return ds.running
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

func sleepWithCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
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
