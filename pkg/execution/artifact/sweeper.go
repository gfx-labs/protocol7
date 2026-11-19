package artifact

import (
	"context"
	"log/slog"
	"time"
)

// SweepEvery is how often RunSweeper applies the retention.
const SweepEvery = time.Hour

// RunSweeper applies the retention every interval until ctx ends. A sweep
// that fails is logged and tried again at the next one.
func (s *Store) RunSweeper(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		interval = SweepEvery
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := s.Sweep(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			logger.Error("artifact retention sweep failed", "error", err)
		case result != (SweepResult{}):
			logger.Info("artifact retention sweep", "versions_removed", result.Versions, "blobs_removed", result.Blobs, "bytes_removed", result.Bytes)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
